import amqp from "amqplib";
import type { ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  PauseKey,
  GameLogSlug,
} from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { subscribeMsgPack } from "../internal/pubsub/subscribe.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ!");

  const confirmChannel: ConfirmChannel = await conn.createConfirmChannel();

  await subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    async (data: GameLog): Promise<AckType> => {
      await writeLog(data);
      process.stdout.write("> ");
      return AckType.Ack;
    },
  );

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    console.log("RabbitMQ connection closed");

    process.exit(0);
  });

  printServerHelp();

  serverHelp: while (true) {
    const words = await getInput();

    if (words.length === 0) {
      continue;
    }

    switch (words[0]) {
      case "pause":
        console.log("Sending pause message...");

        publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });

        break;
      case "resume":
        console.log("Sending resume message...");

        publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });

        break;
      case "quit":
        console.log("Exiting...");
        break serverHelp;
      default:
        console.log("I don't understand that command.");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
