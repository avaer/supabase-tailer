#!/usr/bin/env node

// node supabase-tailer.mjs --jwt [jwt] *.txt

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
import { Debouncer } from 'debouncer-async';

const getCredentialsFromToken = (token) => {
  if (!token) {
    throw new Error("cannot get client for blank token");
  }

  const out = jwt.decode(token);
  const userId = out?.payload?.userId ?? out?.payload?.sub ?? null;
  // const agentId = out?.payload?.agentId ?? null;
  // const mcpId = out?.payload?.mcpId ?? null;
  const payload = out?.payload ?? null;

  if (!userId) {
    throw new Error("could not get user id from token");
  }

  return {
    userId,
    // agentId,
    // mcpId,
    payload,
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
  async addTailStream(p) {
    // ensure the file exists, by opening it in append mode
    // fs.createWriteStream(p, { flags: 'a' });
    // try {
    //   await fs.promises.lstat(p);
    // } catch (err) {
    //   await fs.promises.writeFile(p, '');
    // }

    // get the initial size of the file
    const initialStats = await fs.promises.lstat(p);
    const initialSize = initialStats.size;

    // create the read stream
    const tailStream = createReadStream(p, {
      start: initialSize,
    });
    // tailStream.resume();

    // return the parsed stream
    const stdoutStream = new PassThrough();
    const stderrStream = new PassThrough();
    tailStream.pipe(split2())
      .on('data', (line) => {
        // console.log(`${p}: ${line}`);
        if (line) {
          stdoutStream.write(line + '\n');
        }
      });
    return {
      stdoutStream,
      stderrStream,
    };
  }
}

const main = async () => {
  // Load environment variables from .env file
  dotenv.config();

  // initialize the program
  program
    .name("supabase-tailer")
    .description("Tail multiple files and stream their contents to Supabase")
    .option("--jwt <token>", "JWT token for authentication")
    .option("--tableName <tableName>", "Table name to write to")
    .option("--databaseIdKey <databaseIdKey>", "Database ID key to write to")
    .option("--authIdKey <authIdKey>", "Auth ID key to get the id from")
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
  const tableName = program.opts().tableName;
  if (!tableName) {
    throw new Error("Error: No table name specified");
  }
  const databaseIdKey = program.opts().databaseIdKey;
  if (!databaseIdKey) {
    throw new Error("Error: No database id key specified");
  }
  const authIdKey = program.opts().authIdKey;
  if (!authIdKey) {
    throw new Error("Error: No auth id key specified");
  }
  const { userId, payload } = getCredentialsFromToken(jwt);
  const authId = payload[authIdKey];
  if (!authId) {
    throw new Error("Error: No auth id found in payload");
  }

  // Create a unified stream
  const globalStdoutStream = new PassThrough();
  const globalStderrStream = new PassThrough();
  
  // Set up each file for tailing
  const tailStreamManager = new TailStreamManager();
  const debouncer = new Debouncer();
  const pathPromises = [];
  for (const pathSpec of paths) {
    let tailStream;
    if (pathSpec === '-') {
      tailStream = process.stdin;
    } else {
      const match = pathSpec.match(/^(?:([^:]+):)?([\s\S]*)$/);
      // const format = match[1] || null;
      let p = match[2];
      p = path.resolve(p);

      // use chokidar to watch the glob
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
          const tailStreamPromise = tailStreamManager.addTailStream(p);

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
  const lineQueue = [];
  const makeProcessLogLine = (stream) => (line) => {
    if (line) {
      const entry = {
        user_id: userId,
        [databaseIdKey]: authId,
        content: line,
        stream,
      };
      lineQueue.push(entry);

      debouncer.waitForTurn(async () => {
        const entries = lineQueue.slice();
        lineQueue.length = 0;

        console.log('entries', entries);

        if (entries.length > 0) {
          const numRetries = 10;
          const retryDelayMs = 1000;
          for (let i = 0; i < numRetries; i++) {
            const result = await supabase.from(tableName)
              .insert(entries);
            const { error } = result;
            if (!error) {
              return;
            } else {
              console.warn('log lines insert error', error);
              await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
              continue;
            }
          }
          throw new Error(`failed to insert log after ${numRetries} retries`);
        }
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
