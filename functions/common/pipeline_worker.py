import json
import time
import paho.mqtt.client as mqtt # library that under the hood writes network code with MQTT protocol



class PipelineWorker:
    """
    standardized wrapper, that is the same for every function in the workflow. It manages calls of the paho mqtt client.

    """

    # python classes' attributes don't need prior declaration like other more typed languages.
    # Attributes that are actually declared, are the one that belong to the class itself, rather than the individual instances.
    def __init__(
            self,
            broker_host,
            broker_port,
            subscribe_topic,
            publish_topic, # it is the topic which the next function's pods are subscribed to.
            group_name,
            handler_fn
            ):

        # Handler function is needed?
        self.subscribe_topic = subscribe_topic
        self.publish_topic = publish_topic
        self.group_name = group_name
        self.handler_fn = handler_fn
        self.broker_host = broker_host
        self.broker_port = broker_port

        self.client = mqtt.Client(
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        ) #
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message


    def _on_connect(
            self,
            client,
            userdata,
            flags,
            rc,
            props
            ):

        shared_topic = f"$share/{self.group_name}/{self.subscribe_topic}"
        client.subscribe(shared_topic, qos=1) # QoS is set to 1 and corresponds to "at least one delivery". This is useful in case of operation failures. If mosquitto sends a job to Pod_A, and Pod_A doesn't send back through the TCP connection an ACK, mosquitto is gonna  deduce that the job failed, and is gonna forward again the job to any Pod that is currently alive.

        print(f"[PipelineWorker] Subscribed to {shared_topic}")

    def _on_message(
            self,
            client,
            userdata,
            msg,
            props=None
            ):
        params = json.loads(msg.payload.decode())
        stage = self.group_name

        # Inject timing — before handler........... it's probably useless? unless the workload is different for each request, and the processing time of the function may vary.
        params.setdefault("timestamps", {})
        params["timestamps"][f"{stage}_start"] = time.time()

        # Run the actual logic of the function
        params = self.handler_fn(params, context={})

        # Inject timing — after handler
        params["timestamps"][f"{stage}_end"] = time.time()

        # Forward downstream (if there is a next stage)
        if self.publish_topic:
            self.client.publish(
                self.publish_topic,
                json.dumps(params),
                qos=1
            )

    def run(self):
        self.client.connect(self.broker_host, self.broker_port)
        self.client.loop_forever()
