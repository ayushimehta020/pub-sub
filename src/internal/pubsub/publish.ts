import type { ConfirmChannel } from "amqplib";
import msgpack from "@msgpack/msgpack";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): void {
  const buffer = Buffer.from(JSON.stringify(value));
  ch.publish(exchange, routingKey, buffer, {
    contentType: "application/json",
  });
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const buffer = Buffer.from(msgpack.encode(value));
  ch.publish(exchange, routingKey, buffer, {
    contentType: "application/x-msgpack",
  });
}
