#!/usr/bin/env node

import fs from 'fs';
import dotenv from 'dotenv';
import { program } from 'commander';
import { PassThrough } from 'stream';
import { createReadStream } from 'tail-file-stream';
import split2 from 'split2';
import { createServerClient } from '@supabase/ssr'
import jwt from '@tsndr/cloudflare-worker-jwt';

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

program
  .name("supabase-tailer")
  .description("Tail multiple files and stream their contents to Supabase")
  .option("--jwt <token>", "JWT token for authentication")
  .argument("[files...]", "Files to tail")
  .parse(process.argv);

// const options = program.opts();
const files = program.args;

const main = async () => {
  // Load environment variables from .env file
  dotenv.config();

  if (files.length === 0) {
    console.error("Error: No files specified");
    process.exit(1);
  }

  const jwt = program.opts().jwt;
  if (!jwt) {
    throw new Error("Error: No JWT token specified");
  }
  const { userId, agentId } = getCredentialsFromToken(jwt);

  // Create a unified stream
  const unifiedStream = new PassThrough();
  
  // Set up each file for tailing
  const filePromises = files.map(async (file) => {
    let tailStream;
    if (file === '-') {
      tailStream = process.stdin;
    } else {
      const match = file.match(/^(?:([^:]+):)?([\s\S]*)$/);
      const format = match[1] || null;
      const path = match[2];

      // ensure the file exists, touch it if it doesn't
      try {
        await fs.promises.lstat(path);
      } catch (err) {
        await fs.promises.writeFile(path, '');
      }
      // create the read stream
      tailStream = createReadStream(path);
      // wait for initial eof
      await new Promise((resolve) => {
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
          tailStream.off('eof', oneof);
          tailStream.off('error', onerror);
        };

        tailStream.resume();
      });
    }
    
    tailStream.pipe(unifiedStream, {
      end: false,
    });
  });
  try {
    await Promise.all(filePromises);
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
  unifiedStream.pipe(split2())
    .on('data', (line) => {
      (async () => {
        const o = {
          agent_id: agentId,
          content: line,
        };
        // console.log('inserting log line', o);
        const result = await supabase.from(logsTableName)
          .insert(o);
        const { data, error } = result;
        if (error) {
          console.warn('log insert error', error);
        }
      })();
    });
};

// Run only when this file is executed directly (not when imported as a module)
if (import.meta.url === import.meta.resolve('./supabase-tailer.mjs')) {
  main();
}
