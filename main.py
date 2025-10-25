from etl import ETLProductos, ETLCompras, ETLVentas, DBConnection

def try_connect_db():
    try:
        db = DBConnection(host='localhost', user='root', password='', database='negocio')
        db.connect()
        return db
    except Exception:
        return None

def run_all(db=None):
    jobs = [
        (ETLProductos, "data/productos.sql", ["nombre","codigo","proveedor","categoria","empresa","stock","precio","costo","otro"], "productos_limpio"),
        (ETLCompras, "data/compras.sql", ["fecha","codigo","producto","cantidad","precio_unitario"], "compras_limpio"),
        (ETLVentas, "data/ventas.sql", ["fecha","codigo","producto","cantidad","precio_unitario"], "ventas_limpio"),
    ]
    for cls, file, cols, outname in jobs:
        print("--- Ejecutando:", outname)
        etl = cls(file_path=file, columns=cols, output_name=outname, db_conn=(db.conn if db else None))
        etl.extract()
        etl.transform()
        etl.save_csv()
        etl.save_sqlite()
    print("Finalizado.")

if __name__ == '__main__':
    db = try_connect_db()
    run_all(db)
    if db:
        db.close()
