import uuid
import json
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: Empty msg")
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")
            # self._logger.info(f"{datetime.utcnow()}: TEXT MESSAGE: {msg}")

            order = msg['payload']
            products = dict(msg['payload']['products'][0])

            source_system = "orders-system-kafka"

            user_id = order["user"]["id"]
            user_name = order["user"]["name"]
            user_login = order["user"]["login"]
            user_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(user_id))

            restaurant_id = order["restaurant"]["id"]
            restaurant_name = order["restaurant"]["name"]
            restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(restaurant_id))

            order_id = order["id"]
            order_date = order["date"]
            order_cost = order["cost"]
            order_payment = order["payment"]
            order_status = order["status"]
            order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))

            order_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_pk) + '/' + str(user_pk))

            order_cost_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(order_pk) + '/'
                                             + str(order_cost) + '/' + str(order_payment))
            order_status_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(order_pk) + '/' + str(order_status))
            restaurant_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(restaurant_pk) + '/'
                                                   + str(restaurant_name))
            user_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(user_pk) + '/' + str(user_name) + '/'
                                             + str(user_login))

            self._dds_repository.h_order_insert(
                order_pk,
                order_id,
                order_date,
                datetime.utcnow(),
                source_system)

            self._dds_repository.h_restaurant_insert(
                restaurant_pk,
                restaurant_id,
                datetime.utcnow(),
                source_system)

            self._dds_repository.h_user_insert(
                user_pk,
                user_id,
                datetime.utcnow(),
                source_system)

            for prod in msg['payload']['products']:
                item_product_id = prod["id"]
                item_product_name = prod["name"]
                item_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(item_product_id))

                item_category_name = prod["category"]
                item_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, item_category_name)

                order_item_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_pk) + '/' + str(item_product_pk))

                item_product_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(item_product_pk) + '/'
                                                      + str(item_category_pk))
                item_product_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(item_product_pk) + '/'
                                                        + str(restaurant_pk))

                item_product_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(item_product_pk) + '/'
                                                         + str(item_product_name))

                self._dds_repository.h_category_insert(
                    item_category_pk,
                    item_category_name,
                    datetime.utcnow(),
                    source_system)

                self._dds_repository.h_product_insert(
                    item_product_pk,
                    item_product_id,
                    datetime.utcnow(),
                    source_system)

                self._dds_repository.l_order_product_insert(
                    order_item_product_pk,
                    order_pk,
                    item_product_pk,
                    datetime.utcnow(),
                    source_system)

                self._dds_repository.l_product_category_insert(
                    item_product_category_pk,
                    item_category_pk,
                    item_product_pk,
                    datetime.utcnow(),
                    source_system)

                self._dds_repository.l_product_restaurant_insert(
                    item_product_restaurant_pk,
                    restaurant_pk,
                    item_product_pk,
                    datetime.utcnow(),
                    source_system)

                self._dds_repository.s_product_names_insert(
                    item_product_pk,
                    item_product_name,
                    item_product_names_hashdiff,
                    datetime.utcnow(),
                    source_system)

            self._dds_repository.l_order_user_insert(
                order_user_pk,
                order_pk,
                user_pk,
                datetime.utcnow(),
                source_system)

            self._dds_repository.s_order_cost_insert(
                order_pk,
                order_cost,
                order_payment,
                order_cost_hashdiff,
                datetime.utcnow(),
                source_system)

            self._dds_repository.s_order_status_insert(
                order_pk,
                order_status,
                order_status_hashdiff,
                datetime.utcnow(),
                source_system)

            self._dds_repository.s_restaurant_names_insert(
                restaurant_pk,
                restaurant_name,
                restaurant_names_hashdiff,
                datetime.utcnow(),
                source_system)

            self._dds_repository.s_user_names_insert(
                user_pk,
                user_name,
                user_login,
                user_names_hashdiff,
                datetime.utcnow(),
                source_system)

            dst_msg = {
                "product": self._dds_repository.product_order_cnt_get(),
                "category": self._dds_repository.category_order_cnt_get()
                }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_items(self, order_items) -> List[Dict[str, str]]:
        items = []

        for it in order_items:
            dst_it = {
                "id": it["id"],
                "name": it["name"],
            }
            items.append(dst_it)

        return items

