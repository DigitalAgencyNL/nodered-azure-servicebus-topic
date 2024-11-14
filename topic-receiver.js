
var azure = require('@azure/service-bus');  

const nodeRedNodeTestHelper = require("node-red-node-test-helper");

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


        const myMessageHandler = async (message) => {
        
           
            node.status({ fill: "blue", shape: "ring", text: "received a message" });

            node.send(message);    

            setTimeout(() => { node.status({ fill: "green", shape: "dot", text: "connected" }); }, 2000);
        };
        const myErrorHandler = async (args) => { 
            node.error(args.error);
            node.status({ fill: "red", shape: "ring", text: "error, see debug or output" }); 

        };



        receiver = client.createReceiver(config.topicName, config.subscriptionName, receiverOptions);



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

            node.log("Closing " + d + " - " + node.id);
            await receiver.close();

            await client.close(); 
            done();
        }); 
    }
 
    RED.nodes.registerType("receive-azure-sb-topic", ReceiveMessage);
}