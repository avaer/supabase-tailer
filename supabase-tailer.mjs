#!/usr/bin/env node

import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import { program } from 'commander';
import { PassThrough } from 'stream';
import { createReadStream } from 'tail-file-stream';
import split2 from 'split2';
import { createServerClient } from '@supabase/ssr'
import jwt from '@tsndr/cloudflare-worker-jwt';
import chokidar from 'chokidar';
import { QueueManager } from 'queue-manager-async';

const logsTableName = 'eliza_logs';

const getCredentialsFromToken = (token) => {
  if (!token) {
    throw new Error("cannot get client for blank token");
  }

  const out = jwt.decode(token);
  const userId = out?.payload?.userId ?? out?.payload?.sub ?? null;
  const agentId = out?.payload?.agentId ?? null;

  if (!userId) {
    throw new Error("could not get user id from token");
  }

  return {
    userId,
    agentId,
  };
};

// Create a cookie store that mimics the next/headers cookies API
class LocalCookieStore {
  cookies = new Map();

  get(name) {
    return this.cookies.get(name)?.value;
  }

  getAll() {
    return Array.from(this.cookies.entries()).map(([name, cookie]) => ({
      name,
      value: cookie.value,
      options: cookie.options,
    }));
  }

  set(name, value, options = {}) {
    this.cookies.set(name, { value, options });
  }

  delete(name) {
    this.cookies.delete(name);
  }

  has(name) {
    return this.cookies.has(name);
  }
} 

export const createClient = async ({
  jwt,
} = {}) => {
  if (!process.env.SUPABASE_URL) {
    throw new Error('SUPABASE_URL is not set');
  }
  if (!process.env.SUPABASE_ANON_KEY) {
    throw new Error('SUPABASE_ANON_KEY is not set');
  }
  if (!jwt) {
    throw new Error('JWT is not set');
  }

  const cookieStore = new LocalCookieStore();

  return createServerClient(
    process.env.SUPABASE_URL || '',
    process.env.SUPABASE_ANON_KEY || '',
    {
      cookies: {
        getAll() {
          const allCookies = cookieStore.getAll();
          return allCookies;
        },
        setAll(cookiesToSet) {
          cookiesToSet.forEach(({ name, value, options }) => {
            cookieStore.set(name, value, options);
          });
        },
      },
      global: {
        headers: {
          Authorization: `Bearer ${jwt}`,
        },
      },
    }
  );
};

class TailStreamManager {
  async addTailStream(p, {
    parser,
  }) {
    // ensure the file exists, touch it if it doesn't
    try {
      await fs.promises.lstat(p);
    } catch (err) {
      await fs.promises.writeFile(p, '');
    }
    
    // create the read stream
    const tailStream = createReadStream(p);
    tailStream.resume();

    // wait for initial eof
    await new Promise((resolve, reject) => {
      const oneof = () => {
        resolve(null);
        cleanup();
      };
      tailStream.on('eof', oneof);
      const onerror = (err) => {
        reject(err);
        cleanup();
      };
      tailStream.on('error', onerror);

      const cleanup = () => {
        tailStream.removeListener('eof', oneof);
        tailStream.removeListener('error', onerror);
      };
    });

    // return the parsed stream
    const stdoutStream = new PassThrough();
    const stderrStream = new PassThrough();
    tailStream.pipe(split2())
      .on('data', (line) => {
        console.log(`${p}: ${line}`);
        if (line) {
          const {
            stdout,
            stderr,
          } = parser(line);
          if (stdout) {
            stdoutStream.write(stdout + '\n');
          }
          if (stderr) {
            stderrStream.write(stderr + '\n');
          }
        }
      });
    return {
      stdoutStream,
      stderrStream,
    };
  }
}

const defaultParser = (line) => {
  return {
    stdout: line,
    stderr: null,
  };
};
const dockerJsonLogParser = (line) => {
  const _invalidLogLineError = () => new Error(`log line is not valid: ${line}`);

  try {
    const json = JSON.parse(line);
    const { log, stream } = json;
    if (typeof log === 'string' && ['stdout', 'stderr'].includes(stream)) {
      if (stream === 'stdout') {
        return {
          stdout: log,
          stderr: null,
        };
      } else if (stream === 'stderr') {
        return {
          stdout: null,
          stderr: log,
        };
      } else {
        throw _invalidLogLineError();
      }
    } else {
      throw _invalidLogLineError();
    }
  } catch (err) {
    console.warn(err.stack);
  }
};

const main = async () => {
  // Load environment variables from .env file
  dotenv.config();

  // initialize the program
  program
    .name("supabase-tailer")
    .description("Tail multiple files and stream their contents to Supabase")
    .option("--jwt <token>", "JWT token for authentication")
    .argument("[paths...]", "Paths to tail")
    .parse(process.argv);

  const paths = program.args;
  if (paths.length === 0) {
    console.error("Error: No paths specified");
    process.exit(1);
  }

  const jwt = program.opts().jwt;
  if (!jwt) {
    throw new Error("Error: No JWT token specified");
  }
  const { userId, agentId } = getCredentialsFromToken(jwt);

  // Create a unified stream
  const globalStdoutStream = new PassThrough();
  const globalStderrStream = new PassThrough();
  
  // Set up each file for tailing
  const tailStreamManager = new TailStreamManager();
  const queueManager = new QueueManager();
  const pathPromises = [];
  for (const pathSpec of paths) {
    let tailStream;
    if (pathSpec === '-') {
      tailStream = process.stdin;
    } else {
      const match = pathSpec.match(/^(?:([^:]+):)?([\s\S]*)$/);
      const format = match[1] || null;
      let p = match[2];
      p = path.resolve(p);

      const parser = format === 'json' ? dockerJsonLogParser : defaultParser;

      // use chokidar to watch teh glob
      console.log('watching path', p);
      const watcher = chokidar.watch(p, {
        persistent: true,
        followSymlinks: true,
        awaitWriteFinish: true,
      });

      // // Add debug logging to help diagnose the issue
      // watcher.on('all', (event, path) => {
      //   console.log(`Watcher event: ${event} for ${path}`);
      // });

      watcher.on('add', (p) => {
        (async () => {
          console.log('watching file', p);
          const tailStreamPromise = tailStreamManager.addTailStream(p, {
            parser,
          });

          const {
            stdoutStream,
            stderrStream,
          } = await tailStreamPromise;
          console.log('tailing file', p);
          stdoutStream.pipe(globalStdoutStream, {
            end: false,
          });
          stderrStream.pipe(globalStderrStream, {
            end: false,
          });
        })();
      });
      const watcherPromise = new Promise((resolve) => {
        const onready = () => {
          resolve();
          cleanup();
        };
        watcher.on('ready', onready);

        const cleanup = () => {
          watcher.removeListener('ready', onready);
        };
      });
      pathPromises.push(watcherPromise);
    }
  }
  try {
    await Promise.all(pathPromises);
  } catch (error) {
    console.error(`Failed to tail:`, error);
  }

  // Create a Supabase client to execute the query
  const supabase = await createClient({
    jwt,
  });

  // {
  //   /*
  //     create or replace function get_jwt()
  //     returns text as $$
  //       select auth.jwt();
  //     $$ language sql stable;
  //   */
  //   const result = await supabase.rpc('get_jwt');
  //   console.log('get jwt result', result);
  // }

  // The rest of your code for tailing files
  const makeProcessLogLine = (stream) => (line) => {
    if (line) {
      queueManager.waitForTurn(async () => {
        const numRetries = 10;
        const retryDelayMs = 1000;
        for (let i = 0; i < numRetries; i++) {
          const o = {
            user_id: userId,
            agent_id: agentId,
            content: line,
            stream,
          };
          console.log(o);
          const result = await supabase.from(logsTableName)
            .insert(o);
          const { data, error } = result;
          if (!error) {
            return;
          } else {
            console.warn('log insert error', error);
            await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
            continue;
          }
        }
        throw new Error(`failed to insert log after ${numRetries} retries`);
      });
    }
  };
  globalStdoutStream.pipe(split2())
    .on('data', makeProcessLogLine('stdout'));
  globalStderrStream.pipe(split2())
    .on('data', makeProcessLogLine('stderr'));
};

// Run only when this file is executed directly (not when imported as a module)
if (import.meta.url === import.meta.resolve('./supabase-tailer.mjs')) {
  main();
}
