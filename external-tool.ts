import { tool } from "@mimo-ai/plugin"

export default tool({
  description: "Call a tool supplied by the external OpenAI-compatible agent. Use only tool names and argument shapes listed in the system prompt.",
  args: {
    name: tool.schema.string().describe("Exact external tool name"),
    arguments: tool.schema.string().describe("JSON object encoded as a string"),
  },
  async execute(args, context) {
    const response = await fetch(process.env.MIMO_BRIDGE_TOOL_URL!, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-mimo-bridge-key": process.env.MIMO_BRIDGE_INTERNAL_KEY!,
      },
      body: JSON.stringify({
        sessionID: context.sessionID,
        messageID: context.messageID,
        name: args.name,
        arguments: args.arguments,
      }),
    })
    if (!response.ok) throw new Error(await response.text())
    return await response.text()
  },
})
