import uuid
from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: Empty msg")
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")
            # self._logger.info(f"{datetime.utcnow()}: TEXT MESSAGE: {msg}")

            for cat in msg['category']:
                user_id = uuid.UUID(cat['user_id'])
                category_id = uuid.UUID(cat['category_id'])
                category_name = cat['category_name']
                order_cnt = cat['category_order_cnt']

                self._cdm_repository.user_category_counters_insert(
                    user_id,
                    category_id,
                    category_name,
                    order_cnt)

            for prod in msg['product']:
                user_id = uuid.UUID(prod['user_id'])
                product_id = uuid.UUID(prod['product_id'])
                product_name = prod['product_name']
                order_cnt = prod['product_order_cnt']

                self._cdm_repository.user_product_counters_insert(
                    user_id,
                    product_id,
                    product_name,
                    order_cnt)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
