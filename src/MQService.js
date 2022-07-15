import amqp from "amqplib";
import "dotenv/config";

const CONN_URL = process.env.CLOUD_AMQP_URL;

const mockOperationFunc = () =>
  new Promise((resolve) => {
    const mockOperationTime = 2000;
    setTimeout(() => {
      resolve();
    }, mockOperationTime);
  });

export const publishToQueue = async (queueName, data) =>
  new Promise(async (resolve, reject) => {
    let connection;
    let channel;
    try {
      connection = await amqp.connect(CONN_URL);
      channel = await connection.createChannel();

      await channel.assertQueue(queueName, { durable: true });
      await channel.sendToQueue(queueName, Buffer.from(data), {
        persistent: true,
      });
      console.log("Successfully sent to Queue");

      await channel.close();
      await connection.close();

      resolve({ status: "PUBLISHED" });
    } catch (error) {
      if (channel) await channel.close();
      if (connection) await connection.close();
      console.log(error);
      reject({ status: "FAILED", error });
    }
  });

export const recieveFromQueue = (queueName) =>
  new Promise(async (resolve, reject) => {
    let connection;
    let channel;
    const msgsArr = [];

    try {
      connection = await amqp.connect(CONN_URL);
      channel = await connection.createChannel();

      const { messageCount } = await channel.assertQueue(queueName, {
        durable: true,
      });

      if (messageCount > 0) {
        await channel.consume(
          queueName,
          (msg) => {
            if (msg) {
              msgsArr.push(msg.content.toString());
              channel.ack(msg);
            }
          },
          { noAck: false }
        );

        const { messageCount: consumedMessageCount } =
          await channel.assertQueue(queueName, {
            durable: true,
          });

        if (consumedMessageCount === 0) {
          await channel
            .deleteQueue(queueName, { ifEmpty: true })
            .catch((err) => console.log(err));
        }
      } else {
        console.log("No messages in the Queue");
      }

      await mockOperationFunc(); // mocking I/O operations

      await channel.close();
      await connection.close();

      resolve({ status: "CONSUMED", msgsArr });
    } catch (error) {
      if (channel) await channel.close();
      if (connection) await connection.close();
      console.log(error);
      reject({ status: "FAILED", msgsArr, error });
    }
  });
