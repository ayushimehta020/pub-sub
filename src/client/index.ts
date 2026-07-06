import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerPause } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ!");

  const username = await clientWelcome();

  const [channel, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `pause.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    console.log("RabbitMQ connection closed");

    process.exit(0);
  });

  const gs = new GameState(username);

  await subscribeJSON<PlayingState>(
    conn,
    ExchangePerilDirect,
    `pause.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );

  clientLoop: while (true) {
    const words = await getInput();

    if (words.length === 0) {
      continue;
    }

    switch (words[0]) {
      case "spawn":
        commandSpawn(gs, words);
        break;
      case "move":
        if (commandMove(gs, words)) {
          console.log("Move successful!");
        }
        break;
      case "status":
        await commandStatus(gs);
        break;
      case "help":
        printClientHelp();
        break;
      case "spam":
        console.log("Spamming not allowed yet!");
        break;
      case "quit":
        printQuit();
        break clientLoop;
      default:
        console.log("Unknown command:", words[0]);
        continue;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
