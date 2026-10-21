#!/usr/bin/env node
import { spawn } from 'child_process';
import { join } from 'path';

const PROTO_DIR = join(__dirname, '..', '..', 'proto');
const OUT_DIR = join(__dirname, 'src');

const protos = ['auction.proto', 'signaling.proto', 'market.proto'];

async function generate() {
  console.log('Generating TypeScript types from protobuf...');
  
  for (const proto of protos) {
    await new Promise<void>((resolve, reject) => {
      const args = [
        `--plugin=protoc-gen-ts=${join(__dirname, '..', '..', 'node_modules', '.bin', 'protoc-gen-ts')}`,
        `--ts_out=${OUT_DIR}`,
        `--proto_path=${PROTO_DIR}`,
        proto,
      ];
      
      const proc = spawn('protoc', args, { cwd: PROTO_DIR, stdio: 'inherit' });
      
      proc.on('close', (code) => {
        if (code === 0) {
          console.log(`Generated: ${proto}`);
          resolve();
        } else {
          reject(new Error(`protoc failed with code ${code} for ${proto}`));
        }
      });
    });
  }
  
  console.log('TypeScript types generated successfully!');
}

generate().catch(err => {
  console.error('Generation failed:', err);
  process.exit(1);
});