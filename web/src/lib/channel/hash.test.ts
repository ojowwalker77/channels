import { expect, test } from "bun:test"
import { parseHash } from "./hash"

test("parseHash decodes the fragment, and keeps a malformed one as typed instead of throwing", () => {
  expect(parseHash("#abc%20def")).toEqual({ code: "abc def" })
  expect(parseHash("")).toEqual({ code: "" })
  expect(parseHash("#%")).toEqual({ code: "%" })
  expect(parseHash("#%E0%A4%A ")).toEqual({ code: "%E0%A4%A" })
})
