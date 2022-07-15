import { Router } from "express";
import { publishToQueue, recieveFromQueue } from "./MQService";

const router = Router();

router.get("/", (req, res) => {
  res.send("Router Working");
});

router.post("/publishMsg", async (req, res) => {
  const { queueName, payload } = req.body;
  const isPublished = await publishToQueue(queueName, payload).catch((errObj) =>
    res.status(500).send(errObj)
  );

  if (isPublished) res.status(200).send(isPublished);
});

router.post("/recieveMsg", async (req, res) => {
  const { queueName } = req.body;
  const result = await recieveFromQueue(queueName).catch((errObj) =>
    res.status(500).send(errObj)
  );

  if (result) res.status(200).send(result);
});

export default router;
