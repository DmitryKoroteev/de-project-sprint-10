import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_category_insert(self,
                            h_category_pk: uuid,
                            category_name: str,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_category_pk)s,
                        %(category_name)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_category_pk) DO UPDATE
                    SET
                        category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_order_insert(self,
                            h_order_pk: uuid,
                            order_id: str,
                            order_dt: datetime,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                        h_order_pk,
                        order_id,
                        order_dt,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(order_id)s,
                        %(order_dt)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def h_product_insert(self,
                            h_product_pk: uuid,
                            product_id: str,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(product_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def h_restaurant_insert(self,
                            h_restaurant_pk: uuid,
                            restaurant_id: str,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def h_user_insert(self,
                            h_user_pk: uuid,
                            user_id: str,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(user_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_order_product_insert(self,
                            hk_order_product_pk: uuid,
                            h_order_pk: uuid,
                            h_product_pk: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_product_pk)s,
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'hk_order_product_pk': hk_order_product_pk,
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_order_user_insert(self,
                            hk_order_user_pk: uuid,
                            h_order_pk: uuid,
                            h_user_pk: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_user_pk) DO UPDATE
                    SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'hk_order_user_pk': hk_order_user_pk,
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_product_category_insert(self,
                            hk_product_category_pk: uuid,
                            h_category_pk: uuid,
                            h_product_pk: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                        hk_product_category_pk,
                        h_category_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_category_pk)s,
                        %(h_category_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET
                        h_category_pk = EXCLUDED.h_category_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'hk_product_category_pk': hk_product_category_pk,
                        'h_category_pk': h_category_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_product_restaurant_insert(self,
                            hk_product_restaurant_pk: uuid,
                            h_restaurant_pk: uuid,
                            h_product_pk: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                        hk_product_restaurant_pk,
                        h_restaurant_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(hk_product_restaurant_pk)s,
                        %(h_restaurant_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET
                        h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'hk_product_restaurant_pk': hk_product_restaurant_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_order_cost_insert(self,
                            h_order_pk: uuid,
                            cost: float,
                            payment: float,
                            hk_order_cost_hashdiff: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                        h_order_pk,
                        cost,
                        payment,
                        hk_order_cost_hashdiff,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(cost)s,
                        %(payment)s,
                        %(hk_order_cost_hashdiff)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT ON CONSTRAINT s_order_cost_pk DO UPDATE
                    SET
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'cost': cost,
                        'payment': payment,
                        'hk_order_cost_hashdiff': hk_order_cost_hashdiff,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_order_status_insert(self,
                            h_order_pk: uuid,
                            status: str,
                            hk_order_status_hashdiff: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                        h_order_pk,
                        status,
                        hk_order_status_hashdiff,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_order_pk)s,
                        %(status)s,
                        %(hk_order_status_hashdiff)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT ON CONSTRAINT s_order_status_pk DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'status': status,
                        'hk_order_status_hashdiff': hk_order_status_hashdiff,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_product_names_insert(self,
                            h_product_pk: uuid,
                            name: str,
                            hk_product_names_hashdiff: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                        h_product_pk,
                        name,
                        hk_product_names_hashdiff,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(hk_product_names_hashdiff)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT ON CONSTRAINT s_product_names_pk DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'name': name,
                        'hk_product_names_hashdiff': hk_product_names_hashdiff,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_restaurant_names_insert(self,
                            h_restaurant_pk: uuid,
                            name: str,
                            hk_restaurant_names_hashdiff: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        hk_restaurant_names_hashdiff,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(name)s,
                        %(hk_restaurant_names_hashdiff)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT ON CONSTRAINT s_restaurant_names_pk DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_user_names_insert(self,
                            h_user_pk: uuid,
                            username: str,
                            userlogin: str,
                            hk_user_names_hashdiff: uuid,
                            load_dt: datetime,
                            load_src: str = 'orders-system-kafka'
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                        h_user_pk,
                        username,
                        userlogin,
                        hk_user_names_hashdiff,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(hk_user_names_hashdiff)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT ON CONSTRAINT s_user_names_pk DO UPDATE
                    SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff,
                        load_src = EXCLUDED.load_src
                    ;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'username': username,
                        'userlogin': userlogin,
                        'hk_user_names_hashdiff': hk_user_names_hashdiff,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def product_order_cnt_get(self) -> List[Dict[str, str]]:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT lou.h_user_pk, lop.h_product_pk, spn."name", COUNT(distinct lou.h_order_pk) as 
                    product_cnt_orders
                    from dds.l_order_user lou
                    left join dds.l_order_product lop
                    on lou.h_order_pk = lop.h_order_pk 
                    left join dds.s_product_names spn
                    on lop.h_product_pk = spn.h_product_pk 
                    group by lou.h_user_pk, lop.h_product_pk, spn."name"
                    ;
                    """
                )
                product_rows = cur.fetchall()
                items = []
                for it in product_rows:
                    dst_it = {
                        "user_id": str(it[0]),
                        "product_id": str(it[1]),
                        "product_name": it[2],
                        "product_order_cnt": it[3],
                    }
                    items.append(dst_it)
        return items

    def category_order_cnt_get(self) -> List[Dict[str, str]]:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT lou.h_user_pk, lpc.h_category_pk, hc.category_name, count(distinct lop.h_order_pk) as 
                    category_cnt_orders 
                    from dds.l_order_user lou
                    left join dds.l_order_product lop
                    on lou.h_order_pk = lop.h_order_pk 
                    left join dds.l_product_category lpc 	
                    on lop.h_product_pk = lpc.h_product_pk 
                    left join dds.h_category hc 
                    on lpc.h_category_pk = hc.h_category_pk  
                    group by lou.h_user_pk, lpc.h_category_pk, hc.category_name
                    ;
                    """
                )
                category_rows = cur.fetchall()
                items = []
                for it in category_rows:
                    dst_it = {
                        "user_id": str(it[0]),
                        "category_id": str(it[1]),
                        "category_name": it[2],
                        "category_order_cnt": it[3],
                    }
                    items.append(dst_it)
        return items
