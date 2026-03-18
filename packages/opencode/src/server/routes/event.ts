import { Hono } from "hono"
import { describeRoute, resolver } from "hono-openapi"
import { streamSSE } from "hono/streaming"
import { Effect, Queue, Stream } from "effect"
import { Log } from "@/util/log"
import { BusEvent } from "@/bus/bus-event"
import { Bus } from "@/bus"
import { lazy } from "../../util/lazy"

const log = Log.create({ service: "server" })

export const EventRoutes = lazy(() =>
  new Hono().get(
    "/event",
    describeRoute({
      summary: "Subscribe to events",
      description: "Get events",
      operationId: "event.subscribe",
      responses: {
        200: {
          description: "Event stream",
          content: {
            "text/event-stream": {
              schema: resolver(BusEvent.payloads()),
            },
          },
        },
      },
    }),
    async (c) => {
      log.info("event connected")
      c.header("X-Accel-Buffering", "no")
      c.header("X-Content-Type-Options", "nosniff")
      return streamSSE(c, async (stream) => {
        const body = Stream.callback<string>((q) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              stream.onAbort(() => {
                Queue.endUnsafe(q)
              })

              Queue.offerUnsafe(
                q,
                JSON.stringify({
                  type: "server.connected",
                  properties: {},
                }),
              )

              const unsub = Bus.subscribeAll((event) => {
                Queue.offerUnsafe(q, JSON.stringify(event))
                if (event.type === Bus.InstanceDisposed.type) {
                  Queue.endUnsafe(q)
                }
              })

              // Send heartbeat every 10s to prevent stalled proxy streams.
              const heartbeat = setInterval(() => {
                Queue.offerUnsafe(
                  q,
                  JSON.stringify({
                    type: "server.heartbeat",
                    properties: {},
                  }),
                )
              }, 10_000)

              return { heartbeat, unsub }
            }),
            (x) =>
              Effect.sync(() => {
                clearInterval(x.heartbeat)
                x.unsub()
                Queue.endUnsafe(q)
                log.info("event disconnected")
              }),
          ),
        )

        await Effect.runPromise(
          body.pipe(
            Stream.runForEach((data) =>
              Effect.tryPromise({
                try: () => stream.writeSSE({ data }),
                catch: () => {},
              }),
            ),
          ),
        )
      })
    },
  ),
)
