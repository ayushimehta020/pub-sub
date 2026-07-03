import amqp from "amqplib";
import type { ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ!");

  const confirmChannel: ConfirmChannel = await conn.createConfirmChannel();

  publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
    isPaused: true,
  });

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    console.log("RabbitMQ connection closed");

    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
