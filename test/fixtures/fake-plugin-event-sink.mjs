import { appendFileSync, existsSync, writeFileSync } from 'node:fs';
import readline from 'node:readline';

const mode = process.env.FAKE_SINK_MODE ?? 'ack';
const output = process.env.FAKE_SINK_OUTPUT;
const marker = process.env.FAKE_SINK_MARKER;
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', line => {
  const message = JSON.parse(line);
  if (output) appendFileSync(output, `${JSON.stringify(message)}\n`);
  if (mode === 'exit-once' && marker && !existsSync(marker)) {
    writeFileSync(marker, 'exited');
    process.exit(17);
  }
  if (mode === 'no-ack') return;
  const ack = JSON.stringify({ type: 'ack', generation: message.generation, eventIds: message.events.map(event => event.eventId) }) + '\n';
  if (mode === 'partial') {
    process.stdout.write(ack.slice(0, 5));
    setTimeout(() => process.stdout.write(ack.slice(5)), 5);
  } else {
    process.stdout.write(ack);
    if (mode === 'duplicate') process.stdout.write(ack);
  }
});
