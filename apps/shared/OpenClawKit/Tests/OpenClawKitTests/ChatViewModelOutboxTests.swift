import Foundation
import OpenClawKit
import Testing
@testable import OpenClawChatUI

private func makeOutboxDatabaseURL() throws -> URL {
    let dir = FileManager.default.temporaryDirectory
        .appendingPathComponent("chat-outbox-tests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    return dir.appendingPathComponent("chat-cache.sqlite", isDirectory: false)
}

private func userTexts(_ vm: OpenClawChatViewModel) async -> [String] {
    await MainActor.run {
        vm.messages
            .filter { $0.role == "user" }
            .map { $0.content.compactMap(\.text).joined() }
    }
}

private struct OutboxSendError: Error, LocalizedError {
    var errorDescription: String? { "transport unreachable" }
}

private actor OutboxTransportState {
    var healthy: Bool
    var sendFails: Bool
    var sendRejects = false
    var historyFails = false
    var sentIdempotencyKeys: [String] = []
    var sentMessages: [String] = []
    var sentSessionKeys: [String] = []
    var sentThinkingLevels: [String] = []

    init(healthy: Bool, sendFails: Bool) {
        self.healthy = healthy
        self.sendFails = sendFails
    }

    func setHistoryFails(_ fails: Bool) {
        self.historyFails = fails
    }

    func setHealthy(_ healthy: Bool) {
        self.healthy = healthy
    }

    func setSendFails(_ fails: Bool) {
        self.sendFails = fails
    }

    func setSendRejects(_ rejects: Bool) {
        self.sendRejects = rejects
    }

    func recordSend(sessionKey: String, message: String, idempotencyKey: String, thinking: String) {
        self.sentSessionKeys.append(sessionKey)
        self.sentMessages.append(message)
        self.sentIdempotencyKeys.append(idempotencyKey)
        self.sentThinkingLevels.append(thinking)
    }
}

/// Scripted transport for offline-outbox flows: health is switchable, sends
/// can be forced to fail, and history synthesizes the durable user rows for
/// every accepted send (what the gateway would persist).
private final class OutboxTestTransport: @unchecked Sendable, OpenClawChatTransport {
    let state: OutboxTransportState
    private let stream: AsyncStream<OpenClawChatTransportEvent>
    private let continuation: AsyncStream<OpenClawChatTransportEvent>.Continuation

    init(healthy: Bool, sendFails: Bool = false) {
        self.state = OutboxTransportState(healthy: healthy, sendFails: sendFails)
        var cont: AsyncStream<OpenClawChatTransportEvent>.Continuation!
        self.stream = AsyncStream { c in cont = c }
        self.continuation = cont
    }

    func goOnline() async {
        await self.state.setHealthy(true)
        self.continuation.yield(.health(ok: true))
    }

    func emit(_ event: OpenClawChatTransportEvent) {
        self.continuation.yield(event)
    }

    func requestHistory(sessionKey: String) async throws -> OpenClawChatHistoryPayload {
        guard await self.state.healthy, await !self.state.historyFails else { throw OutboxSendError() }
        let keys = await self.state.sentIdempotencyKeys
        let texts = await self.state.sentMessages
        let durableUserRows = zip(keys.indices, keys).map { index, key in
            AnyCodable([
                "role": "user",
                "content": [["type": "text", "text": index < texts.count ? texts[index] : ""]],
                "timestamp": Double(1000 + index),
                "__openclaw": ["idempotencyKey": "\(key):user"],
            ] as [String: Any])
        }
        return OpenClawChatHistoryPayload(
            sessionKey: sessionKey,
            sessionId: "sess-live",
            messages: durableUserRows,
            thinkingLevel: "off")
    }

    func sendMessage(
        sessionKey: String,
        message: String,
        thinking: String,
        idempotencyKey: String,
        attachments _: [OpenClawChatAttachmentPayload]) async throws -> OpenClawChatSendResponse
    {
        if await self.state.sendFails {
            throw OutboxSendError()
        }
        if await self.state.sendRejects {
            // Gateway responded but refused to start the run.
            return OpenClawChatSendResponse(runId: idempotencyKey, status: "error")
        }
        await self.state.recordSend(
            sessionKey: sessionKey,
            message: message,
            idempotencyKey: idempotencyKey,
            thinking: thinking)
        return OpenClawChatSendResponse(runId: idempotencyKey, status: "accepted")
    }

    func listSessions(limit _: Int?) async throws -> OpenClawChatSessionsListResponse {
        OpenClawChatSessionsListResponse(ts: nil, path: nil, count: 0, defaults: nil, sessions: [])
    }

    func requestHealth(timeoutMs _: Int) async throws -> Bool {
        await self.state.healthy
    }

    func events() -> AsyncStream<OpenClawChatTransportEvent> {
        self.stream
    }
}

private func makeOutboxViewModel(
    transport: OutboxTestTransport,
    outbox: any OpenClawChatCommandOutbox,
    transcriptCache: (any OpenClawChatTranscriptCache)? = nil,
    retryDelaysMs: [UInt64] = [1, 1]) async -> OpenClawChatViewModel
{
    await MainActor.run {
        let vm = OpenClawChatViewModel(
            sessionKey: "main",
            transport: transport,
            transcriptCache: transcriptCache,
            outbox: outbox)
        vm.outboxRetryDelaysMs = retryDelaysMs
        return vm
    }
}

private func sendWhileOffline(_ vm: OpenClawChatViewModel, text: String) async throws {
    await MainActor.run {
        vm.input = text
        vm.send()
    }
    try await waitUntil("queued bubble for \(text)") {
        await MainActor.run {
            vm.messages.contains { message in
                message.role == "user" && message.content.contains { $0.text == text }
            }
        }
    }
}

@MainActor
private func queuedStateCount(_ vm: OpenClawChatViewModel) -> Int {
    vm.outboxStatesByMessageID.count
}

struct ChatViewModelOutboxTests {
    @Test func `offline send queues durably and renders queued row`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "hello offline")

        // Nothing hit the transport; the command is durable instead.
        #expect(await transport.state.sentIdempotencyKeys.isEmpty)
        let commands = await store.loadCommands()
        #expect(commands.map(\.text) == ["hello offline"])
        #expect(commands.map(\.status) == [.queued])
        #expect(commands.map(\.sessionKey) == ["main"])

        // The visible row carries the queued state and the draft was cleared.
        #expect(await MainActor.run { vm.input.isEmpty })
        let queuedStates = await MainActor.run {
            vm.messages.compactMap { vm.outboxState(for: $0.id) }
        }
        #expect(queuedStates == [.queued])

        // Recreating the view model (fresh cold open, still offline)
        // restores the queued bubble from the durable store.
        let vm2 = await makeOutboxViewModel(transport: transport, outbox: store)
        await MainActor.run { vm2.load() }
        try await waitUntil("queued bubble restored after recreation") {
            await MainActor.run {
                vm2.messages.contains { vm2.outboxState(for: $0.id) == .queued }
            }
        }
        #expect(await userTexts(vm2) == ["hello offline"])
    }

    @Test func `reconnect flushes queued commands in order with their idempotency keys`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        // Same store instance backs cache and outbox, like the app wiring.
        let vm = await makeOutboxViewModel(transport: transport, outbox: store, transcriptCache: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "first")
        try await sendWhileOffline(vm, text: "second")
        let queuedIDs = await store.loadCommands().map(\.id)
        #expect(queuedIDs.count == 2)

        await transport.goOnline()

        try await waitUntil("outbox drained") {
            await store.loadCommands().isEmpty
        }
        // At-least-once contract: the transport saw each command exactly once
        // here, keyed by its client UUID, in strict createdAt order.
        #expect(await transport.state.sentIdempotencyKeys == queuedIDs)
        #expect(await transport.state.sentMessages == ["first", "second"])
        #expect(await transport.state.sentSessionKeys == ["main", "main"])

        // Durable history replaced the queued bubbles without duplicating
        // them, and no outbox state markers remain.
        try await waitUntil("durable history reconciled") {
            await MainActor.run { vm.sessionId == "sess-live" }
        }
        #expect(await userTexts(vm) == ["first", "second"])
        #expect(await MainActor.run { queuedStateCount(vm) } == 0)

        // Crash-window durability: the sent turns were written through to the
        // transcript cache no later than outbox-row deletion, so a cold
        // offline reopen still shows them.
        let cached = await store.loadTranscript(sessionKey: "main")
        let cachedUserTexts = cached
            .filter { $0.role == "user" }
            .map { $0.content.compactMap(\.text).joined() }
        #expect(cachedUserTexts == ["first", "second"])
    }

    @Test func `assistant reply for a flushed run lands via the external-run final event`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "question")
        await transport.goOnline()
        try await waitUntil("outbox drained") {
            await store.loadCommands().isEmpty
        }
        let runId = try #require(await transport.state.sentIdempotencyKeys.first)

        // Drop history availability so the assertion below can only be
        // satisfied by the event path, not a lucky history refresh. (The
        // scripted history never contains assistant rows, so leaving it on
        // would wipe the appended final with an incomplete snapshot.)
        await transport.state.setHistoryFails(true)

        // Flushed runs are intentionally not in pendingRuns; the reply is
        // delivered through the session-scoped external-run final branch.
        transport.emit(
            .chat(
                OpenClawChatEventPayload(
                    runId: runId,
                    sessionKey: "main",
                    state: "final",
                    message: AnyCodable([
                        "role": "assistant",
                        "content": [["type": "text", "text": "answer"]],
                        "timestamp": 5000.0,
                    ] as [String: Any]),
                    errorMessage: nil)))

        try await waitUntil("assistant reply visible") {
            await MainActor.run {
                vm.messages.contains { message in
                    message.role == "assistant" && message.content.contains { $0.text == "answer" }
                }
            }
        }
    }

    @Test func `sent turn is cached before the outbox row is deleted`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store, transcriptCache: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "must survive")

        // Sends succeed on reconnect but history stays unreachable, so the
        // post-flush history write-through can never populate the cache.
        // Only the pre-delete write-through can preserve the turn.
        await transport.state.setHistoryFails(true)
        await transport.goOnline()
        try await waitUntil("outbox drained") {
            await store.loadCommands().isEmpty
        }

        let cached = await store.loadTranscript(sessionKey: "main")
        #expect(cached.map { $0.content.compactMap(\.text).joined() } == ["must survive"])
        _ = vm
    }

    @Test func `gateway rejections burn attempts then fail terminally and support tap retry`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "doomed")

        // Gateway is reachable again but rejects the run on every attempt.
        await transport.state.setSendRejects(true)
        await transport.goOnline()

        try await waitUntil("command failed after max attempts") {
            await store.loadCommands().map(\.status) == [.failed]
        }
        let failed = try #require(await store.loadCommands().first)
        #expect(failed.retryCount == OpenClawChatViewModel.maxOutboxSendAttempts)
        #expect(failed.lastError != nil)
        try await waitUntil("failed state visible") {
            await MainActor.run {
                vm.messages.contains { vm.outboxState(for: $0.id)?.isFailed == true }
            }
        }

        // Tap-to-retry resets attempts; with the gateway accepting again the
        // command now flushes and the row disappears.
        await transport.state.setSendRejects(false)
        let failedMessageID = try #require(await MainActor.run {
            vm.messages.first { vm.outboxState(for: $0.id)?.isFailed == true }?.id
        })
        await MainActor.run { vm.retryOutboxMessage(failedMessageID) }
        try await waitUntil("retried command drained") {
            await store.loadCommands().isEmpty
        }
        #expect(await transport.state.sentIdempotencyKeys.count == 1)
    }

    @Test func `transport failures keep commands queued without burning attempts`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        // Health reads true, but the actual send path is down: the send gate
        // is bypassed and the transport error must requeue instead of losing
        // the draft.
        let transport = OutboxTestTransport(healthy: true, sendFails: true)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await waitUntil("bootstrap healthy") {
            await MainActor.run { vm.healthOK }
        }
        await MainActor.run {
            vm.input = "stale health send"
            vm.send()
        }

        // The optimistic bubble survives as a durable outbox row that stays
        // queued: connectivity blips never burn retry attempts.
        try await waitUntil("send requeued durably") {
            await store.loadCommands().map(\.status) == [.queued]
        }
        // Give the automatic retry chain a few cycles to prove the row never
        // escalates toward failed while the transport keeps throwing. The
        // status may read 'sending' mid-attempt; the invariants are that it
        // never turns failed and attempts stay unburned.
        try await Task.sleep(nanoseconds: 50_000_000)
        let requeued = try #require(await store.loadCommands().first)
        #expect(requeued.status != .failed)
        #expect(requeued.retryCount == 0)
        #expect(requeued.text == "stale health send")
        #expect(await userTexts(vm) == ["stale health send"])
        let bubbleKey = await MainActor.run {
            vm.messages.first { $0.role == "user" }?.idempotencyKey
        }
        // Same idempotency identity as the failed live send, so a duplicate
        // delivery on the gateway side stays deduped.
        #expect(bubbleKey == "\(requeued.id):user")

        // Once the transport recovers, the next healthy transition drains
        // the row without any user action. (Repeated throws exhaust the
        // retry ladder and drop health, so recovery is signaled the same way
        // a real reconnect is.)
        await transport.state.setSendFails(false)
        await transport.goOnline()
        try await waitUntil("requeued command drained") {
            await store.loadCommands().isEmpty
        }
        #expect(await transport.state.sentIdempotencyKeys == [requeued.id])
    }

    @Test func `tap retry refreshes createdAt so an expired command can resend`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        // A command that sat offline past the staleness bound.
        let staleCreatedAt = Date().timeIntervalSince1970 -
            OpenClawChatSQLiteTranscriptCache.outboxCommandMaxAge - 60
        #expect(await store.enqueueCommand(
            OpenClawChatOutboxCommand(
                id: "c-expired",
                sessionKey: "main",
                text: "old message",
                thinking: "off",
                createdAt: staleCreatedAt,
                status: .queued,
                retryCount: 0,
                lastError: nil)))
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        // Restore surfaces the expired command as failed("expired").
        try await waitUntil("expired command visible as failed") {
            await MainActor.run {
                vm.messages.contains { vm.outboxState(for: $0.id)?.isFailed == true }
            }
        }
        #expect(await store.loadCommands().map(\.lastError) == [
            OpenClawChatSQLiteTranscriptCache.outboxExpiredError,
        ])

        // Explicit retry is new intent: createdAt refreshes, so the row goes
        // back to queued instead of immediately re-expiring, and it flushes
        // once the gateway is reachable.
        let messageID = try #require(await MainActor.run {
            vm.messages.first { vm.outboxState(for: $0.id)?.isFailed == true }?.id
        })
        await MainActor.run { vm.retryOutboxMessage(messageID) }
        try await waitUntil("retried command re-queued") {
            await store.loadCommands().map(\.status) == [.queued]
        }
        await transport.goOnline()
        try await waitUntil("expired-then-retried command drained") {
            await store.loadCommands().isEmpty
        }
        #expect(await transport.state.sentMessages == ["old message"])
    }

    @Test func `flush sends the thinking level captured with the queued command`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        // Queued earlier under a session running "high"; the visible session's
        // current level is "off" and must not leak into the flush.
        #expect(await store.enqueueCommand(
            OpenClawChatOutboxCommand(
                id: "c-think",
                sessionKey: "other-session",
                text: "think hard",
                thinking: "high",
                createdAt: Date().timeIntervalSince1970,
                status: .queued,
                retryCount: 0,
                lastError: nil)))
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)
        #expect(await MainActor.run { vm.thinkingLevel } == "off")

        await MainActor.run { vm.load() }
        await transport.goOnline()
        try await waitUntil("outbox drained") {
            await store.loadCommands().isEmpty
        }
        #expect(await transport.state.sentThinkingLevels == ["high"])
        #expect(await transport.state.sentSessionKeys == ["other-session"])
        _ = vm
    }

    @Test func `flushed background-session turn is spliced into its cached transcript`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        // Queued for a session the user is no longer viewing.
        #expect(await store.enqueueCommand(
            OpenClawChatOutboxCommand(
                id: "c-background",
                sessionKey: "other-session",
                text: "sent from elsewhere",
                thinking: "off",
                createdAt: Date().timeIntervalSince1970,
                status: .queued,
                retryCount: 0,
                lastError: nil)))
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store, transcriptCache: store)

        await MainActor.run { vm.load() }
        await transport.goOnline()
        try await waitUntil("outbox drained") {
            await store.loadCommands().isEmpty
        }

        // The turn survives in that session's cached transcript even though
        // its messages were never loaded into the view model.
        let cached = await store.loadTranscript(sessionKey: "other-session")
        #expect(cached.map { $0.content.compactMap(\.text).joined() } == ["sent from elsewhere"])
        #expect(cached.map(\.idempotencyKey) == ["c-background:user"])
        _ = vm
    }

    @Test func `full queue refuses enqueue and keeps the draft`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        for index in 0..<OpenClawChatSQLiteTranscriptCache.maxQueuedCommands {
            let accepted = await store.enqueueCommand(
                OpenClawChatOutboxCommand(
                    id: "prefill-\(index)",
                    sessionKey: "other",
                    text: "m\(index)",
                    thinking: "off",
                    createdAt: Date().timeIntervalSince1970,
                    status: .queued,
                    retryCount: 0,
                    lastError: nil))
            #expect(accepted)
        }
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run {
            vm.input = "does not fit"
            vm.send()
        }
        try await waitUntil("refusal surfaced") {
            await MainActor.run { vm.errorText != nil }
        }
        // The draft survives so the text is not lost, and no row was added.
        #expect(await MainActor.run { vm.input } == "does not fit")
        #expect(await userTexts(vm).isEmpty)
        #expect(await store.loadCommands().count == OpenClawChatSQLiteTranscriptCache.maxQueuedCommands)
    }

    @Test func `repeated transport failures climb the backoff ladder then drop health`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false, sendFails: true)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "stuck in transit")

        // Gateway reports healthy but every send throws: the flush must walk
        // the retry ladder (streak 1, 2) and then drop health instead of
        // retrying at the first rung forever.
        await transport.goOnline()
        // healthOK starts false pre-goOnline, so the exhaustion signal is
        // the streak walking past the ladder (2 rungs in tests) WITH health
        // down again — not the initial offline state.
        try await waitUntil("ladder exhausted and health dropped") {
            await MainActor.run { vm.outboxTransportFailureStreak >= 3 && !vm.healthOK }
        }
        // Row survives as queued: transport throws never burn durable
        // attempts.
        let commands = await store.loadCommands()
        #expect(commands.map(\.status) == [.queued])
        #expect(commands.map(\.retryCount) == [0])

        // A genuine recovery flushes normally and resets the streak.
        await transport.state.setSendFails(false)
        await transport.goOnline()
        try await waitUntil("command sent after recovery") {
            await transport.state.sentIdempotencyKeys.count == 1
        }
        try await waitUntil("row drained") {
            await store.loadCommands().isEmpty
        }
        #expect(await MainActor.run { vm.outboxTransportFailureStreak } == 0)
    }

    @Test func `deleting a queued message removes bubble and durable row`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "changed my mind")

        let messageID = try #require(await MainActor.run {
            vm.messages.first { vm.outboxState(for: $0.id) == .queued }?.id
        })
        await MainActor.run { vm.deleteOutboxMessage(messageID) }

        #expect(await userTexts(vm).isEmpty)
        #expect(await MainActor.run { queuedStateCount(vm) } == 0)
        try await waitUntil("durable row deleted") {
            await store.loadCommands().isEmpty
        }
    }
}

/// Holds `deleteCommand` until released so tests can pin the exact window
/// where a user delete races an in-flight flush pass (row still visible in
/// the pass's snapshot).
private final class HeldDeleteOutbox: @unchecked Sendable, OpenClawChatCommandOutbox {
    private let base: OpenClawChatSQLiteTranscriptCache
    private let gate = DeleteGate()

    init(base: OpenClawChatSQLiteTranscriptCache) {
        self.base = base
    }

    func releaseHeldDeletes() async {
        await self.gate.open()
    }

    /// Fired (once) just before the claim forwards, on the flush's task:
    /// lets tests land a user delete inside the claim's await window.
    private var onClaim: (@Sendable () async -> Void)?

    func setOnClaim(_ hook: @escaping @Sendable () async -> Void) {
        self.onClaim = hook
    }

    func enqueueCommand(_ command: OpenClawChatOutboxCommand) async -> Bool {
        await self.base.enqueueCommand(command)
    }

    func loadCommands() async -> [OpenClawChatOutboxCommand] {
        await self.base.loadCommands()
    }

    func recoverInterruptedSends() async {
        await self.base.recoverInterruptedSends()
    }

    @discardableResult
    func markCommandSending(id: String) async -> Bool {
        if let hook = self.onClaim {
            self.onClaim = nil
            await hook()
        }
        return await self.base.markCommandSending(id: id)
    }

    func markCommandQueued(id: String, retryCount: Int, lastError: String?) async {
        await self.base.markCommandQueued(id: id, retryCount: retryCount, lastError: lastError)
    }

    func markCommandFailed(id: String, retryCount: Int, lastError: String?) async {
        await self.base.markCommandFailed(id: id, retryCount: retryCount, lastError: lastError)
    }

    func markCommandRetried(id: String) async {
        await self.base.markCommandRetried(id: id)
    }

    func deleteCommand(id: String) async {
        await self.gate.wait()
        await self.base.deleteCommand(id: id)
    }
}

private actor DeleteGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func open() {
        self.isOpen = true
        for waiter in self.waiters { waiter.resume() }
        self.waiters.removeAll()
    }

    func wait() async {
        if self.isOpen { return }
        await withCheckedContinuation { self.waiters.append($0) }
    }
}

extension ChatViewModelOutboxTests {
    @Test func `double submit during the offline health probe enqueues once`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: store)

        await MainActor.run { vm.load() }
        // Two rapid submits of the same draft: the second lands while the
        // first is still awaiting the forced health probe. The isSending
        // guard must swallow it instead of enqueueing a duplicate row.
        await MainActor.run {
            vm.input = "tap tap"
            vm.send()
            vm.send()
        }
        try await waitUntil("queued bubble for tap tap") {
            await MainActor.run {
                vm.messages.contains { message in
                    message.role == "user" && message.content.contains { $0.text == "tap tap" }
                }
            }
        }

        let commands = await store.loadCommands()
        #expect(commands.map(\.text) == ["tap tap"])
        #expect(await MainActor.run { queuedStateCount(vm) } == 1)
    }

    @Test func `deleting during the claim await never sends`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let outbox = HeldDeleteOutbox(base: store)
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: outbox)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "deleted inside the claim")
        let messageID = try #require(await MainActor.run {
            vm.messages.first { vm.outboxState(for: $0.id) == .queued }?.id
        })

        // The delete lands inside markCommandSending's await window: after
        // the pre-claim tombstone check, before the claim resolves. The
        // post-claim recheck must catch it.
        outbox.setOnClaim {
            await MainActor.run { vm.deleteOutboxMessage(messageID) }
        }
        await transport.goOnline()
        try await waitUntil("flush drains without sending") {
            await MainActor.run { queuedStateCount(vm) == 0 }
        }
        #expect(await transport.state.sentIdempotencyKeys.isEmpty)
        await outbox.releaseHeldDeletes()
        try await waitUntil("durable row deleted") {
            await store.loadCommands().isEmpty
        }
    }

    @Test func `deleting a queued bubble mid-flush never sends it`() async throws {
        let url = try makeOutboxDatabaseURL()
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let store = OpenClawChatSQLiteTranscriptCache(databaseURL: url, gatewayID: "gw-test")
        let outbox = HeldDeleteOutbox(base: store)
        let transport = OutboxTestTransport(healthy: false)
        let vm = await makeOutboxViewModel(transport: transport, outbox: outbox)

        await MainActor.run { vm.load() }
        try await sendWhileOffline(vm, text: "changed my mind mid-flush")

        // User deletes the bubble; the durable row deletion is held, so the
        // next flush pass still sees the row in its snapshot — the exact
        // race window the tombstone protects.
        let messageID = try #require(await MainActor.run {
            vm.messages.first { vm.outboxState(for: $0.id) == .queued }?.id
        })
        await MainActor.run { vm.deleteOutboxMessage(messageID) }

        await transport.goOnline()
        try await waitUntil("flush pass drains without sending") {
            await MainActor.run { queuedStateCount(vm) == 0 }
        }
        #expect(await transport.state.sentIdempotencyKeys.isEmpty)

        // Once the held deletion completes, the row is gone for good and a
        // later flush still sends nothing.
        await outbox.releaseHeldDeletes()
        try await waitUntil("durable row deleted") {
            await store.loadCommands().isEmpty
        }
        await transport.emit(.health(ok: true))
        #expect(await transport.state.sentIdempotencyKeys.isEmpty)
    }
}
