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
 * @param {Array}  [opts.templates]      Template Definitions, as JSON strings
 * @param {string} [opts.primaryScada]   Primary Host Application ID. When set,
 *                                       store-forward is enabled so the node
 *                                       gates NBIRTH on STATE (see README).
 * @param {string} [opts.username]
 * @param {string} [opts.password]
 * @returns {Array} Node-RED flow
 */

// A device metric of this type is a *named* template: the dataType is the
// definition's name rather than the literal "Template" (see isNamedTemplate in
// mqtt-sparkplug-plus.js:368). Exported so run-tck.js declares the metric with
// the same name it is defined under here.
const TEMPLATE_NAME = "TckTemplate";

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
			// On, so the TCK actually exercises aliases. The node supports them,
			// and with them off payloads-alias-uniqueness is never reached - it is
			// only recorded inside `if (current.hasAlias())`
			// (SessionEstablishmentTest.checkPayloadsAliasAndNameRequirement), and
			// payloads-alias-birth-requirement passes vacuously.
			aliasMetrics: true,
			// A Template Definition, published in NBIRTH. Without one the whole
			// payloads-template-* group is unreachable: every check is gated on a
			// metric whose datatype is Template. The node emits whatever is
			// configured here via getTemplates() (mqtt-sparkplug-plus.js:1067).
			templates: opts.templates || [
				JSON.stringify({
					name: TEMPLATE_NAME,
					type: "Template",
					value: {
						version: "1.0.0",
						isDefinition: true,
						// MUST cover every member any instance will ever carry
						// (tck-id-payloads-template-definition-members). Member names
						// may themselves contain "/" - the node resolves flat paths by
						// longest matching prefix.
						metrics: [
							{ name: "speed", type: "Int32" },
							{ name: "torque", type: "Int32" },
							{ name: "status/running", type: "Boolean" }
						],
						// Present so the parameter assertions execute at all: the TCK
						// only records definition-parameters, -parameters-default and
						// instance-parameters when parameters exist. The node copies
						// these onto each instance (mqtt-sparkplug-plus.js:251).
						parameters: [{ name: "ratio", type: "Int32", value: 2 }]
					}
				})
			],
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

module.exports = { buildFlow, TEMPLATE_NAME };
