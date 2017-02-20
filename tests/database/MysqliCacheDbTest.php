<?php
namespace Jepsonwu\database;

use Jepsonwu\database\mysql\MysqliCacheDb;
use PHPUnit\Framework\TestCase;

/**
 *
 *
 * Created by PhpStorm.
 * User: jepsonwu
 * Date: 2017/2/14
 * Time: 16:56
 */
class MysqliCacheDbTest extends TestCase
{
    /**
     * @return MysqliCacheDb
     */
    public function testDb()
    {
        $db = new MysqliCacheDb("10.10.106.218", "root", "tortdh_gogo888!", "inchat_user", 3306);
        $db->configCache([
            "memcached", "11211"
        ])->configTable("user", "id")->registerFilterInsertFunc([$this, "filterInsert"]);
        $db->registerFilterShowFunc([$this, "filterShow"]);

        return $db;
    }

    public function filterInsert($data)
    {
        $single = false;
        if (!is_array(current($data))) {
            $data = [$data];
            $single = true;
        }

        foreach ($data as &$item) {
            isset($item['school']) && $item['school'] .= "filter";
        }

        return $single ? current($data) : $data;
    }

    public function filterShow($data)
    {
        $single = false;
        if (!is_array(current($data))) {
            $data = [$data];
            $single = true;
        }
        foreach ($data as &$item) {
            isset($item['school']) && $item['school'] = substr($item['school'], 0, -6);
        }

        return $single ? current($data) : $data;
    }

    /**
     * @depends testDb
     * @param MysqliCacheDb $db
     * @return bool
     */
    public function testInsert(MysqliCacheDb $db)
    {
        $data = [
            "token" => md5(microtime(true)),
            "gender" => rand(0, 1) == 1 ? "男" : "女",
            "birthday" => "1990-12-" . rand(10, 20),
            "school" => "第" . rand(1, 2) . "中学",
            "city" => "杭州",
            "gps" => "120.125433,30.272932",
            "ip" => "10.10.105.31",
            "personal_tag" => 1,
            "created_at" => time()
        ];
        $last_id = $db->insertData($data);
        $this->assertTrue($last_id !== false, "insert data failed");

        return $last_id;
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testFetchByPrimaryCache($primary, MysqliCacheDb $db)
    {
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue(isset($result['id']) && $result['id'] == $primary, "fetch by primary cache failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testDisableCache($primary, MysqliCacheDb $db)
    {
        $db->disableCache();
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue(isset($result['id']) && $result['id'] == $primary, "disable cache failed");

        $db->updateByPrimaryCache($primary, ["ip" => "127.0.0.1"]);
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue(isset($result['ip']) && $result['ip'] == "127.0.0.1", "disable cache fetch failed");

        $db->enableCache();
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue(isset($result['ip']) && $result['ip'] == "127.0.0.1", "enable cache fetch failed");
    }

    /**
     * @depends testDb
     * @param MysqliCacheDb $db
     */
    public function testUpdate(MysqliCacheDb $db)
    {
        $result = $db->update("user", ["city" => "shanghai"]);
        $this->assertTrue($result === false, "update must return false");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testUpdateByPrimaryCache($primary, MysqliCacheDb $db)
    {
        $result = $db->updateByPrimaryCache($primary, ["city" => "shanghai"]);
        $this->assertTrue($result == true, "update by primary cache failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testFetchOneByCache($primary, MysqliCacheDb $db)
    {
        $db->where("id", $primary);
        $result = $db->fetchOneByCache();

        $this->assertTrue(isset($result['id']) && $result['id'] == $primary, "fetch one by cache failed");
        $this->assertTrue(isset($result['city']) && $result['city'] == "shanghai", "fetch one ofter update by primary cache failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testUpdateByCache($primary, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), $primary);
        $result = $db->updateByCache(["city" => "hangzhou"]);
        $this->assertTrue($result === 1, "update by cache failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testFetchOneByCacheUpdate($primary, MysqliCacheDb $db)
    {
        $db->where("id", $primary);
        $result = $db->column("city")->fetchOneByCache();
        $this->assertTrue(isset($result['city']) && count($result) == 1, "fetch one by cache update failed");
        $this->assertTrue(isset($result['city']) && $result['city'] == "hangzhou", "fetch one ofter update by cache failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testIncrease($primary, MysqliCacheDb $db)
    {
        $db->where("id", $primary);
        $result = $db->increase($primary, "personal_tag");
        $this->assertTrue($result, "increase failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testIncreaseFetch($primary, MysqliCacheDb $db)
    {
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue($result && $result['personal_tag'] == 2, "increase and fetch failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testDecrease($primary, MysqliCacheDb $db)
    {
        $db->where("id", $primary);
        $result = $db->decrease($primary, "personal_tag");
        $this->assertTrue($result, "decrease failed");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testDecreaseFetch($primary, MysqliCacheDb $db)
    {
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue($result && $result['personal_tag'] == 1, "decrease and fetch failed");
    }

    /**
     * @depends testDb
     * @param MysqliCacheDb $db
     * @return bool
     */
    public function testInsertAgain(MysqliCacheDb $db)
    {
        $data = [
            "token" => md5(microtime(true)),
            "gender" => rand(0, 1) == 1 ? "男" : "女",
            "birthday" => "1990-12-" . rand(10, 20),
            "school" => "第" . rand(1, 2) . "中学",
            "city" => "杭州",
            "gps" => "120.125433,30.272932",
            "ip" => "10.10.105.31",
            "created_at" => time()
        ];
        $last_id = $db->insertData($data);
        $this->assertTrue($last_id !== false, "insert again data failed");

        return $last_id;
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchByPrimaryArrCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $result = $db->fetchByPrimaryArrCache([$primary, $primaryAgain]);
        $this->assertTrue($result && $result[0]['id'] == $primary && $result[1]['id'] == $primaryAgain, "fetch by primary arr cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchPairsByPrimaryArrCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $result = $db->column("id,gender")->fetchPairsByPrimaryArrCache([$primary, $primaryAgain]);
        $this->assertTrue($result && key($result) == $primary && next($result) && key($result) == $primaryAgain, "fetch pairs by primary arr cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchAssocByPrimaryArrCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $result = $db->fetchAssocByPrimaryArrCache([$primary, $primaryAgain]);
        $this->assertTrue($result && key($result) == $primary && $result[$primary]["id"] == $primary, "fetch assoc by primary arr cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchAllByCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->fetchAllByCache();
        $this->assertTrue($result && $result[0]['id'] == $primary && $result[1]['id'] == $primaryAgain, "fetch all by cache failed");

        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $db->paginateByLimit(1, 1);
        $result = $db->fetchAllByCache();
        $this->assertTrue($result && count($result) == 1 && $result[0]['id'] == $primary, "fetch all by cache and paginate by limit failed");

        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $db->paginateByPrimary($primaryAgain + 1, 1);
        $result = $db->fetchAllByCache();
        $this->assertTrue($result && count($result) == 1 && $result[0]['id'] == $primaryAgain, "fetch all by cache and paginate by primary failed");

        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $db->orderByPrimary();
        $result = $db->fetchAllByCache();
        $this->assertTrue($result && $result[0]['id'] == $primaryAgain && $result[1]['id'] == $primary, "fetch all by cache and order by primary failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchPairsByCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->column("id,gender")->fetchPairsByCache();
        $this->assertTrue($result && key($result) == $primary && next($result) && key($result) == $primaryAgain, "fetch pairs by cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testFetchAssocByCache($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->fetchAssocByCache();
        $this->assertTrue($result && key($result) == $primary && $result[$primary]['id'] == $primary, "fetch assoc by cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testCount($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->count();
        $this->assertTrue($result == 2, "count failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testCountDistinct($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->countDistinct();
        $this->assertTrue($result == 2, "count distinct failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testCountDistinctColumn($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primary, $primaryAgain], "in");
        $result = $db->countDistinctColumn("gps");
        $this->assertTrue($result == 1, "count distinct column failed");
    }

    /**
     * @depends testDb
     * @param MysqliCacheDb $db
     */
    public function testRawQuery(MysqliCacheDb $db)
    {
        $result = $db->rawQuery("");
        $this->assertTrue($result == false, "raw query must be return false");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testTransaction($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $db->startTransaction();
        try {
            $db->updateByPrimaryCache($primary, ["personal_tag" => 3]);
            $db->updateByPrimaryCache($primaryAgain, ["personal_tag" => 3]);

            throw new \Exception("error");
        } catch (\Exception $e) {
            $db->rollback();
        }
        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue($result && $result['personal_tag'] == 1, "tracnaction rollback failed");

        $db->startTransaction();
        try {
            $db->updateByPrimaryCache($primary, ["personal_tag" => 3]);
            $db->updateByPrimaryCache($primaryAgain, ["personal_tag" => 3]);

            $db->commit();
        } catch (\Exception $e) {
            $db->rollback();
        }

        $result = $db->fetchByPrimaryCache($primary);
        $this->assertTrue($result && $result['personal_tag'] == 3, "tracnaction commit failed");

        $result = $db->fetchByPrimaryCache($primaryAgain);
        $this->assertTrue($result && $result['personal_tag'] == 3, "tracnaction commit failed");
    }

    /**
     * @depends testDb
     * @param MysqliCacheDb $db
     */
    public function testDelete(MysqliCacheDb $db)
    {
        $result = $db->delete("user", []);
        $this->assertTrue($result == false, "delete must be return false");
    }

    /**
     * @depends testInsert
     * @depends testDb
     * @param $primary
     * @param MysqliCacheDb $db
     */
    public function testDeleteByPrimaryCache($primary, MysqliCacheDb $db)
    {
        $result = $db->deleteByPrimaryCache($primary);
        $this->assertTrue($result, "delete by primary cache failed");
    }

    /**
     * @depends testInsertAgain
     * @depends testDb
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testDeleteByCache($primaryAgain, MysqliCacheDb $db)
    {
        $db->where($db->getPrimaryKey(), [$primaryAgain], "in");
        $result = $db->deleteByCache();
        $this->assertTrue($result == 1, "delete by cache failed");
    }

    /**
     * @depends testInsert
     * @depends testInsertAgain
     * @depends testDb
     * @param $primary
     * @param $primaryAgain
     * @param MysqliCacheDb $db
     */
    public function testDeleteFetch($primary, $primaryAgain, MysqliCacheDb $db)
    {
        $result = $db->fetchByPrimaryArrCache([$primary, $primaryAgain]);
        $this->assertTrue(empty($result), "delete fetch failed");
    }
}