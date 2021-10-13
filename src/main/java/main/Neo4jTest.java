package main;


import org.neo4j.driver.*;

import static org.neo4j.driver.Values.parameters;

/**
 * @ClassName Neo4jTest
 * @Deacription java操作neo4j
 * @Author GoldenStar
 * @Date 2021/10/13 14:18
 * @Version 1.0
 **/
public class Neo4jTest {
    public static void main(String[] args) {
        // 加载驱动
        Driver driver = GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "root"));
        // 获取连接
        Session session = driver.session();

        // 统计总数
        int count = count(session, "MATCH (n) return count(n)");
        System.out.println("统计总数为：" + count);

        // 添加
        long id = createPerson(session, "测试添加");
        System.out.println("添加id为：" + id);

        // 查询名称
        String name = getPersonById(session, id);
        System.out.println("添加名称为：" + name);

        // 修改
        updatePerson(session, id, "测试修改");

        // 查询名称
        name = getPersonById(session, id);
        System.out.println("修改后名称为：" + name);

        // 删除
        deletePerson(session, id);

        // 执行查询
        queryPerson(session);

        // 释放资源
        session.close();
        driver.close();
    }

    /**
     * 根据id查询名称
     *
     * @param session session
     * @param id    id
     * @return name 名称
     */
    private static String getPersonById(Session session, long id) {
        return session.readTransaction(transaction -> {
            Result result = transaction.run("MATCH (n:person) WHERE id(n) = $id return n.name", parameters("id", id));
            return result.single().get(0).asString();
        });
    }

    /**
     * 查询person
     *
     * @param session session
     */
    private static void queryPerson(Session session) {
        Result result = session.run("MATCH (n:person) " +
                "RETURN id(n) as id," +
                "n.name as name," +
                "n.actor as actor");
        while (result.hasNext()) {
            Record record = result.next();
            int id = record.get("id").asInt();
            String name = record.get("name").asString();
            String actor = record.get("actor").asString();
            System.out.println("id：" + id + "，name：" + name + "，actor：" + actor);
        }
    }

    /**
     * 根据id修改名称
     * @param session session
     * @param id id
     * @param name name
     */
    private static void updatePerson(Session session, long id, String name) {
        // 执行修改
        session.writeTransaction(transaction -> {
            transaction.run("MATCH (n) WHERE id(n) = $id SET n.name = $name", parameters("id", id, "name", name));
            return null;
        });
    }

    /**
     * 根据id删除person
     *
     * @param session session
     * @param id    id
     */
    private static void deletePerson(Session session, long id) {
        // 执行添加
        session.writeTransaction(transaction -> {
            transaction.run("MATCH (n) WHERE id(n) = $id DELETE n", parameters("id", id));
            return null;
        });
    }

    /**
     * 添加person
     *
     * @param session session
     * @param name    名称
     * @return id
     */
    private static long createPerson(Session session, String name) {
        // 执行添加
        session.writeTransaction(transaction -> {
            transaction.run("CREATE (a:person {name: $name})", parameters("name", name));
            return null;
        });

        return session.readTransaction(transaction -> {
            Result result = transaction.run("MATCH (a:person {name: $name}) RETURN id(a)", parameters("name", name));
            return result.single().get(0).asLong();
        });
    }

    /**
     * 统计
     *
     * @param session session
     * @param query   查询语句
     * @return 数量
     */
    private static int count(Session session, String query) {
        return session.readTransaction(transaction -> transaction.run(query, Values.parameters()).single().get(0).asInt());
    }
}
