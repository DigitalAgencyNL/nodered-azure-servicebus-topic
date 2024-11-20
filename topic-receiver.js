var azure = require('@azure/service-bus');

module.exports = function (RED) {

    var client;
    var receiver;
    var running = false;

    var statusEnum = {
        disconnected: { color: "red", text: "Disconnected" },
        connected: { color: "green", text: "Connected" },
        sent: { color: "yellow", text: "Sent message" },
        received: { color: "blue", text: "Message received" },
        error: { color: "grey", text: "Error" }
    };

    var setStatus = function (node, status) {
        node.status({ fill: status.color, shape: "dot", text: status.text });  
    }

    var myMessageHandler = async (node, message) => {
        if (node.connectivityFeedbackTimeout) {
            clearTimeout(node.connectivityFeedbackTimeout);
        }
        node.connectivityFeedbackTimeout = setTimeout(function () { 
            setStatus(node, statusEnum.connected);
        }, 2000);

        setStatus(node, statusEnum.received);

        console.log("Message received: ", message.body);

        node.send({ payload: message.body });
    };


    var myErrorHandler = async (node, args) => {
        node.error(args.error);
        setStatus(node, statusEnum.error);
    };

    var connectToServiceBus = (node, config) => {

        node.log("Connecting to Azure Service Bus Topic: " + config.topic);

        let client = new azure.ServiceBusClient(config.connectionString);

        let receiverOptions = {
            receiveMode: 'receiveAndDelete'  // Receive and delete the message   
        }


        let receiver = client.createReceiver(config.topic, config.subscription, receiverOptions);

        let subscribeOptions = {
            autoComplete: false,
            maxConcurrentCalls: 1
        };

        node.close = receiver.subscribe({
            processMessage: async (msg) => await myMessageHandler(node, msg),
            processError:  async (args) => await myErrorHandler(node, args)
        }, subscribeOptions);

        node.client = client;   
        node.receiver = receiver;   

        setStatus(node, statusEnum.connected);

    }

    var disconnectFromServiceBus = function (node) {
        if (node.reconnectTimer) {
            clearTimeout(node.reconnectTimer);
            node.reconnectTimer = null;
        }

        if (node.close) {
            node.close();
            node.close = null;
        }

        if (node.client) {
            node.log('Disconnecting from Azure Service Bus');
            node.client.close();
            node.client = null;
            node.receiver.close();
            node.receiver = null;
            setStatus(node, statusEnum.disconnected);
        }
    };


    // Main function called by Node-RED    
    function ReceiveMessage(config) {

        RED.nodes.createNode(this, config);

        var node = this;

        running = true; // flag to indicate the node is running 

        node.config = config; // save the config so we can access it later

        connectToServiceBus(this, config);

        node.on("close", function () {
            disconnectFromServiceBus(node, this);
        });

    }




    RED.nodes.registerType("receive-azure-sb-topic", ReceiveMessage);
}