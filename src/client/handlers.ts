import type { ConfirmChannel } from "amqplib";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  confirmChannel: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    const result = handleMove(gs, move);
    process.stdout.write("> ");

    switch (result) {
      case MoveOutcome.Safe:
        return AckType.Ack;
      case MoveOutcome.MakeWar:
        const rw: RecognitionOfWar = {
          attacker: move.player,
          defender: gs.getPlayerSnap(),
        };
        try {
          publishJSON(
            confirmChannel,
            ExchangePerilTopic,
            `${WarRecognitionsPrefix}.${gs.getUsername()}`,
            rw,
          );
        } catch (err) {
          console.log("Failed to publish war declaration:", err);
          return AckType.NackRequeue;
        }

        return AckType.Ack;
      case MoveOutcome.SamePlayer:
      default:
        return AckType.NackDiscard;
    }
  };
}

export function handlerWar(
  gs: GameState,
  confirmChannel: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar): Promise<AckType> => {
    const resolution = handleWar(gs, rw);
    process.stdout.write("> ");

    switch (resolution.result) {
      case WarOutcome.NotInvolved:
        return AckType.NackRequeue;
      case WarOutcome.NoUnits:
        return AckType.NackDiscard;
      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
        try {
          await publishGameLog(
            confirmChannel,
            gs.getUsername(),
            `${resolution.winner} won a war against ${resolution.loser}`,
          );
          return AckType.Ack;
        } catch (err) {
          console.log("Failed to publish game log:", err);
          return AckType.NackRequeue;
        }
      case WarOutcome.Draw:
        try {
          await publishGameLog(
            confirmChannel,
            gs.getUsername(),
            `A war between ${rw.attacker} and ${rw.defender} resulted in a draw`,
          );
          return AckType.Ack;
        } catch (err) {
          console.log("Failed to publish game log:", err);
          return AckType.NackRequeue;
        }
      default:
        console.log("Unknown war outcome:", resolution);
        return AckType.NackDiscard;
    }
  };
}
