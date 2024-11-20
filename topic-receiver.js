
var azure = require('@azure/service-bus');

module.exports = async function (RED) {

    let client;
    let receiver;

    function ReceiveMessage(config) {

        RED.nodes.createNode(this, config);

        var node = this;

        node.log("Creating receive-message-topic node ");

        client = new azure.ServiceBusClient(config.connectionString);

        let receiverOptions = {
            receiveMode: 'receiveAndDelete'  // Receive and delete the message   
        }

        let connectivityFeedbackTimeout;

        const myMessageHandler = async (message) => { 
            if (connectivityFeedbackTimeout) {
                clearTimeout(connectivityFeedbackTimeout);
            }

            node.status({ fill: "blue", shape: "ring", text: "message received" });
            connectivityFeedbackTimeout = setTimeout(() => { node.status({ fill: "green", shape: "dot", text: "connected" }); }, 2000);
            console.log("Message received: ", message.body);
            node.send({ payload:  message.body });   

        };
        const myErrorHandler = async (args) => {
            node.error(args.error);
            node.status({ fill: "red", shape: "ring", text: "error, see debug or output" });
        };


        receiver = client.createReceiver(config.topic, config.subscription, receiverOptions);

        let subscribeOptions = {
            autoComplete: false,
            maxConcurrentCalls: 1
        };

        receiver.subscribe({
            processMessage: myMessageHandler,
            processError: myErrorHandler
        }, subscribeOptions);

        node.log("Subribed and listening for messages");

        node.on("close", async function (done) {
            node.log("Closing topic receiver - " + node.id);
            await receiver.close();
            await client.close();
            done();
        });
    }

    RED.nodes.registerType("receive-azure-sb-topic", ReceiveMessage);
}