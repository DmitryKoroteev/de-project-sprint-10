import uuid
from typing import List, Dict

from lib.pg import PgConnect


# from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self,
                                      user_id: uuid,
                                      category_id: uuid,
                                      category_name: str,
                                      order_cnt: int,
                                      ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                    VALUES(
                        %(user_id)s,
                        %(category_id)s,
                        %(category_name)s,
                        %(order_cnt)s
                    )
                    ON CONFLICT ON CONSTRAINT user_category_counters_un DO UPDATE
                    SET
                        order_cnt = EXCLUDED.order_cnt
                    ;
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )

    def user_product_counters_insert(self,
                                     user_id: uuid,
                                     product_id: uuid,
                                     product_name: str,
                                     order_cnt: int,
                                     ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                    VALUES(
                        %(user_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        %(order_cnt)s
                    )
                    ON CONFLICT ON CONSTRAINT user_product_counters_un DO UPDATE
                    SET
                        order_cnt = EXCLUDED.order_cnt
                    ;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )


