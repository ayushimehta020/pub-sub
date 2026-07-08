import type { ConfirmChannel } from "amqplib";
import { publishGameLog } from "../../client/index.js";
import { publishMsgPack } from "../pubsub/publish.js";
import { ExchangePerilTopic, GameLogSlug } from "../routing/routing.js";
import { getMaliciousLog } from "./gamelogic.js";
import type { GameLog } from "./logs.js";

export async function commandSpam(
  confirmChannel: ConfirmChannel,
  username: string,
  words: string[],
): Promise<void> {
  if (words.length < 2 || words[1] === undefined) {
    throw new Error("Usage: spam <number>");
  }

  const count = parseInt(words[1], 10);

  if (isNaN(count) || count < 1) {
    throw new Error("Please provide a valid positive number.");
  }

  for (let i = 0; i < count; i++) {
    await publishGameLog(confirmChannel, username, getMaliciousLog());
  }
}
