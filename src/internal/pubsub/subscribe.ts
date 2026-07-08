import amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./consume.js";
import msgpack from "@msgpack/msgpack";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (buffer: Buffer) => JSON.parse(buffer.toString()) as T,
  );
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (buffer: Buffer) => msgpack.decode(buffer) as T,
  );
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
  );

  await channel.consume(
    queue.queue,
    async (message: amqp.ConsumeMessage | null) => {
      if (message === null) {
        return;
      }

      const data = deserializer(message.content);

      const ackType = await handler(data);

      switch (ackType) {
        case AckType.Ack:
          channel.ack(message);
          console.log("Ack!");
          break;
        case AckType.NackRequeue:
          channel.nack(message, false, true);
          console.log("NackRequeue!");
          break;
        case AckType.NackDiscard:
          channel.nack(message, false, false);
          console.log("NackDiscard!");
          break;
      }
    },
  );
}
