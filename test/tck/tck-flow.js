/**
 * Node-RED flow fixture used by the Sparkplug TCK harness (run-tck.js).
 *
 * Shaped after `simpleFlow` in test/sparkplug_device__spec.js, but parameterized
 * so each TCK test can pick its own group / edge node / device IDs and decide
 * whether the Primary Host (STATE) gate is active.
 */

/**
 * Build a flow containing an EoN broker node, one device and one generic
 * sparkplug-out node (used to emit NDATA in the SendData test).
 *
 * @param {object} opts
 * @param {string} opts.groupId          Sparkplug Group ID
 * @param {string} opts.edgeNodeId       Sparkplug Edge Node ID
 * @param {string} opts.deviceId         Sparkplug Device ID
 * @param {string} opts.brokerHost       broker hostname
 * @param {string|number} opts.brokerPort broker port
 * @param {object} [opts.metrics]        device metric definitions
 * @param {string} [opts.primaryScada]   Primary Host Application ID. When set,
 *                                       store-forward is enabled so the node
 *                                       gates NBIRTH on STATE (see README).
 * @param {string} [opts.username]
 * @param {string} [opts.password]
 * @returns {Array} Node-RED flow
 */
function buildFlow(opts) {
	const usePrimaryHost = typeof opts.primaryScada === "string" && opts.primaryScada !== "";

	return [
		{
			id: "device",
			type: "mqtt sparkplug device",
			name: opts.deviceId,
			// The TCK's edge tests only require that whatever we birth, we then
			// report consistently, so a pair of simple metrics is enough.
			// Note this node only publishes DBIRTH once *every* configured
			// metric has a value, so the harness must feed all of them.
			metrics: opts.metrics || {
				"test/int": { dataType: "Int32" },
				"test/bool": { dataType: "Boolean" }
			},
			broker: "broker",
			wires: [[]]
		},
		{
			id: "out",
			type: "mqtt sparkplug out",
			name: "raw out",
			topic: "",
			qos: "0",
			retain: false,
			broker: "broker"
		},
		{
			id: "broker",
			type: "mqtt-sparkplug-broker",
			name: "TCK broker",
			deviceGroup: opts.groupId,
			eonName: opts.edgeNodeId,
			broker: opts.brokerHost,
			port: String(opts.brokerPort),
			clientid: "",
			usetls: false,
			protocolVersion: "4",
			keepalive: "60",
			cleansession: true,
			enableStoreForward: usePrimaryHost,
			primaryScada: usePrimaryHost ? opts.primaryScada : "",
			username: opts.username || "",
			password: opts.password || ""
		}
	];
}

module.exports = { buildFlow };
