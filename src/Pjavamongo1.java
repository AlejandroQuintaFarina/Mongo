
/*

Este código es un ejemplo de cómo conectarse a una base de datos MongoDB utilizando Java y la biblioteca de MongoDB para Java.
El código define varias funciones que permiten conectarse a una base de datos, consultar y actualizar registros, e insertar nuevos documentos.
A continuación se explica cada una de las funciones:

conectar_a_servidor(): Crea un cliente de MongoDB y se conecta a un servidor MongoDB en la dirección localhost y el puerto 27017.

conectar_a_base(String nomebase): Conecta a una base de datos de MongoDB llamada nomebase.

conectar_a_unha_colecion(String coleccion): Conecta a una colección dentro de la base de datos.

consultar_por_id(ObjectId id): Consulta un registro en la colección utilizando un ObjectId como identificador.

consultar_por_campo_valor(String campo, Object valor): Consulta registros en la colección utilizando un campo y valor específicos.

actualizar_por_id(ObjectId id, String campo, Object valor): Actualiza un registro en la colección utilizando un ObjectId como identificador
y un campo y valor específicos.

incrementar_por_id(ObjectId id, String campo, Object valor): Incrementa el valor de un campo en un registro en la colección utilizando un ObjectId como identificador.

main(String[] args): Función principal que llama a las demás funciones para realizar operaciones en la base de datos,
como actualizar, consultar e insertar documentos.

El código utiliza objetos básicos de MongoDB como BasicDBObject para definir consultas y actualizaciones, y utiliza la clase Document de la biblioteca MongoDB
para Java para representar documentos BSON de MongoDB.
También utiliza el método find() para buscar documentos en la base de datos y el método insertOne() para insertar un nuevo documento.


 */


package pjavamongo1;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.types.ObjectId;
import java.util.logging.Logger;
import java.util.logging.Level;

public class Pjavamongo1 {

    public static MongoClient client;
    public static MongoDatabase database;
    public static MongoCollection<Document> colecion;

    public static void conectar_a_servidor() {
        client = new MongoClient("localhost", 27017);
    }

    public static void conectar_a_base(String nomebase) {
        database = client.getDatabase(nomebase);
    }

    public static void conectar_a_unha_colecion(String coleccion) {
        colecion = database.getCollection(coleccion);
    }
    public static void  consultar_por_id(ObjectId id){
        BasicDBObject condicion= new BasicDBObject("_id",id);
//        Document d = colecion.find(condicion).projection(Projections.include("_id","kind","score")).first();
//        ObjectId _idj = d.getObjectId("_id");
//        String kind = d.getString("kind");
//        Double score = d.getDouble("score");
//        System.out.println("_id:"+_idj+", kind: "+ kind + " score: "+score);
        BasicDBObject campos = new BasicDBObject();
        campos.put("kind",1);
        campos.put("score",1);
        Document d = colecion.find(condicion).projection(campos).first();
        ObjectId _idj = d.getObjectId("_id");
        String kind = d.getString("kind");
        Double score = d.getDouble("score");
        System.out.println("_id:"+_idj+", kind: "+ kind + " score: "+score);
    }
    public static void  consultar_por_campo_valor(String campo, Object valor){
        BasicDBObject condicion = new BasicDBObject("kind","essay")
                .append("student",new BasicDBObject("$gt",0)
                        .append("$lt",3));

        // BasicDBObject condicion= new BasicDBObject(campo,valor);
        FindIterable<Document> docs = colecion.find(condicion);
        MongoCursor<Document> iterator = docs.iterator();
        while (iterator.hasNext()){
            Document d = iterator.next();
            ObjectId _idj = d.getObjectId("_id");
            String kind  = d.getString("kind");
            Double score = d.getDouble("score");
            Double student = d.getDouble("student");
            System.out.println("_id:"+_idj+", kind: "+ kind + " score: "+score + "student: "+ student);
        }

    }
    public static void  actualizar_por_id(ObjectId id, String campo, Object valor){
        colecion.updateOne(new BasicDBObject("_id", id),new BasicDBObject("$set", new BasicDBObject(campo,valor)));
    }

    public static void incrementar_por_id(ObjectId id, String campo, Object valor){
        colecion.updateOne(new BasicDBObject("_id", id),new BasicDBObject("$inc", new BasicDBObject(campo,valor)));
    }





    public static void main(String[] args) {
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);
        conectar_a_servidor();
        conectar_a_base("caso");
        conectar_a_unha_colecion("casas");
        // actualizar_por_id(new ObjectId("4c90f2543d937c033f42482c"),"score",75.0);
        //incrementar_por_id(new ObjectId("4c90f2543d937c033f42482c"),"score",-5.0);
//consultar_por_campo_valor("student",99.0);
        //consultar_por_id(new ObjectId("4c90f2543d937c033f42482c"));
        Document engadir = new Document("kind", "taller")
                .append("score", 61.0)
                .append("enderezo", new Document()
                        .append("rua", "urzaiz")
                        .append("numero", 4)
                        .append("cidade", "vigo")
                );
        colecion.insertOne(engadir);

//      colecion.updateOne(new BasicDBObject("kind","taller"),new BasicDBObject("$set",new BasicDBObject("score",98.0)));
//
//
//      colecion.deleteOne(new BasicDBObject("kind","taller"));
        client.close();

    }

}