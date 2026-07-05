import Foundation
import OpenClawKit

enum GatewayOnboardingReset {
    @MainActor
    static func prepareForBootstrapPairing(
        appModel: NodeAppModel,
        instanceId: String,
        defaults: UserDefaults = .standard) async
    {
        await appModel.purgeChatTranscriptCache()
        self.clearPairingState(appModel: appModel, instanceId: instanceId, defaults: defaults)
    }

    @MainActor
    static func reset(
        appModel: NodeAppModel,
        instanceId: String,
        defaults: UserDefaults = .standard) async
    {
        await self.prepareForBootstrapPairing(appModel: appModel, instanceId: instanceId, defaults: defaults)
        self.clearOnboardingState(defaults: defaults)
    }

    /// The debug launch flag must finish before startup reads pairing defaults.
    /// No cache actor exists yet, so the startup-only file purge is synchronous.
    @MainActor
    static func resetBeforeStartup(
        appModel: NodeAppModel,
        instanceId: String,
        defaults: UserDefaults = .standard)
    {
        appModel.purgeChatTranscriptCacheBeforeStartup()
        self.clearPairingState(appModel: appModel, instanceId: instanceId, defaults: defaults)
        self.clearOnboardingState(defaults: defaults)
    }

    @MainActor
    private static func clearPairingState(
        appModel: NodeAppModel,
        instanceId: String,
        defaults: UserDefaults)
    {
        appModel.disconnectGateway()

        let trimmedInstanceId = instanceId.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedInstanceId.isEmpty {
            GatewaySettingsStore.deleteGatewayCredentials(instanceId: trimmedInstanceId)
        }

        let deviceId = DeviceIdentityStore.loadOrCreate().deviceId
        DeviceAuthStore.clearToken(deviceId: deviceId, role: "node")
        DeviceAuthStore.clearToken(deviceId: deviceId, role: "operator")
        DeviceAuthStore.clearAll(profile: .shareExtension)

        GatewaySettingsStore.clearLastGatewayConnection(defaults: defaults)
        GatewaySettingsStore.clearPreferredGatewayStableID(defaults: defaults)
        GatewaySettingsStore.clearLastDiscoveredGatewayStableID(defaults: defaults)
        GatewayTLSStore.clearAllFingerprints()
        defaults.set(false, forKey: "gateway.autoconnect")
    }

    private static func clearOnboardingState(defaults: UserDefaults) {
        OnboardingStateStore.reset(defaults: defaults)

        defaults.set(false, forKey: "gateway.onboardingComplete")
        defaults.set(false, forKey: "gateway.hasConnectedOnce")
        defaults.set(false, forKey: "gateway.manual.enabled")
        defaults.set("", forKey: "gateway.manual.host")
        defaults.set("", forKey: "gateway.setupCode")
        defaults.set(defaults.integer(forKey: "onboarding.requestID") + 1, forKey: "onboarding.requestID")
    }
}
