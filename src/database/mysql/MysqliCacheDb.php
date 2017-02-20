<?php
namespace Jepsonwu\database\mysql;

use Jepsonwu\database\cache\Cache;
use Jepsonwu\database\cache\MemcachedCache;
use Jepsonwu\database\exception\MysqlException;

/**
 * support single data cache
 * support multi data cache
 * support transcation
 * support custom component,like "stat cache hit rate"
 * support register custom cache driver,like redis
 * support register data filter
 *
 * Created by PhpStorm.
 * User: jepsonwu
 * Date: 2017/2/6
 * Time: 16:29
 */
class MysqliCacheDb extends \MysqliDb
{
    /**
     * table
     * @var
     */
    private $tableName;
    private $primaryKey;//just support single primary key todo multi primary key

    /**
     * @var Cache
     */
    private $cache;
    private $enableCache = true;
    private $cacheConfig;
    private $expiredTime = 86400;

    /**
     * CURD
     * @var string
     */
    private $columns = "*";
    private $paginate = null;

    /**
     * transaction
     * @var bool
     */
    private $inTransaction = false;
    private $transactionDeleteCachePrimaryArr = [];


    /**
     * data filter
     * @var
     */
    private $filterInsertFunc;
    private $filterShowFunc;

    public function configTable($tableName, $primaryKey)
    {
        $this->tableName = $tableName;
        $this->primaryKey = $primaryKey;
        return $this;
    }

    public function setExpiredTime($time)
    {
        $this->expiredTime = (int)$time;
        return $this;
    }

    public function configCache(array $config)
    {
        $this->cacheConfig = !is_array(current($config)) ? [$config] : $config;;
        return $this;
    }

    /**
     * you can disable cache in develop environment or switch cache between enable and disable anytime,it'll be safely.
     * @return $this
     */
    public function disableCache()
    {
        $this->enableCache = false;
        $this->deleteCachePrefix();

        return $this;
    }

    public function enableCache()
    {
        $this->enableCache = true;
        return $this;
    }

    /**
     * you must be set another cache instance what implements cache interface if you don't use memcached cache
     * @param Cache $cache
     * @return $this
     */
    public function setCache(Cache $cache)
    {
        $this->cache = $cache;
        return $this;
    }

    /**
     * @param callable $func
     * @return $this
     */
    public function registerFilterInsertFunc(callable $func)
    {
        $this->filterInsertFunc = $func;
        return $this;
    }

    /**
     * @param callable $func
     * @return $this
     */
    public function registerFilterShowFunc(callable $func)
    {
        $this->filterShowFunc = $func;
        return $this;
    }

    public function getTableName()
    {
        if (!$this->tableName)
            throw new MysqlException("Mysql db tablename invalid!");

        return $this->tableName;
    }

    public function getPrimaryKey()
    {
        if (!$this->primaryKey)
            throw new MysqlException("Mysql db primary key invalid!");

        return $this->primaryKey;
    }

    /**
     * @return Cache
     * @throws MysqlException
     */
    private function getCache()
    {
        if (!$this->cache) {
            if (!$this->cacheConfig)
                throw new MysqlException("Mysql db cache config invalid!");

            try {
                $this->cache = new MemcachedCache($this->cacheConfig);
            } catch (\Exception $e) {
                throw new MysqlException("Mysql db cache invalid,error:" . $e->getMessage());
            }
        }

        return $this->cache;
    }

    private function getCachePrefixKey()
    {
        return md5($this->host . $this->port . $this->db . $this->getTableName());
    }

    private function deleteCachePrefix()
    {
        $this->getCache()->delete($this->getCachePrefixKey());
    }

    private function getCachePrefix()
    {
        $prefix = "";
        if (!$this->getCache()->get($this->getCachePrefixKey())) {
            $prefix = md5(microtime(true) . $this->getCachePrefixKey());
            $this->getCache()->set($this->getCachePrefixKey(), $prefix, 0);
        }

        return $prefix;
    }

    private function getCacheKey($primary)
    {
        return md5($this->getCachePrefix() . "_" . $primary);
    }

    private function deleteCache(array $primaryArr)
    {
        return $this->getCache()->deleteMulti(array_map(function ($primary) {
            return $this->getCacheKey($primary);
        }, $primaryArr));
    }

    /**
     * set select columns,array also ok
     * @param string $columns
     * @return $this
     */
    public function column($columns = "*")
    {
        $this->columns = $columns;
        return $this;
    }

    private function filterColumn($result)
    {
        if ($this->columns == "*")
            return $this->filterShow($result);

        $columns = is_array($this->columns) ? $this->columns : explode(",", $this->columns);
        $columns = array_combine($columns, array_fill(0, count($columns), 1));

        $result && $result = array_intersect_key($result, $columns);

        $this->resetFilter();
        return $this->filterShow($result);
    }

    protected function resetFilter()
    {
        $this->columns = "*";
        $this->paginate = null;
    }

    protected function filterShow($data)
    {
        is_callable($this->filterShowFunc) && $data = call_user_func_array($this->filterShowFunc, [$data]);
        return $data;
    }

    public function fetchByPrimary($primary)
    {
        $this->where($this->getPrimaryKey(), $primary);
        return $this->filterShow($this->fetchOne());
    }

    public function fetchByPrimaryCache($primary)
    {
        if (!$this->enableCache)
            return $this->fetchByPrimary($primary);

        $result = $this->getCache()->get($this->getCacheKey($primary));
        //todo stat cache hit rate

        if (empty($result)) {
            $result = $this->where($this->getPrimaryKey(), $primary)->getOne($this->getTableName());
            $result && $this->getCache()->set($this->getCacheKey($primary), json_encode($result), $this->expiredTime);
        } else {
            $result = json_decode($result, true);
        }

        return empty($result) ? [] : $this->filterColumn($result);
    }

    public function fetchOne()
    {
        $result = $this->getOne($this->getTableName(), $this->columns);
        $this->resetFilter();
        return empty($result) ? [] : $this->filterShow($result);
    }

    public function fetchOneByCache()
    {
        if (!$this->enableCache)
            return $this->fetchOne();

        $result = [];
        $primary = $this->getValue($this->getTableName(), $this->getPrimaryKey());
        $primary && $result = $this->fetchByPrimaryCache($primary);

        return $result;
    }

    public function fetchByPrimaryArr(array $primaryArr)
    {
        $this->where($this->getPrimaryKey(), $primaryArr, "in");
        return $this->filterShow($this->fetchAll());
    }

    /**
     * remain sequence
     * @param array $primaryArr
     * @return array
     */
    public function fetchByPrimaryArrCache(array $primaryArr)
    {
        if (!$this->enableCache)
            return $this->fetchByPrimary($primaryArr);

        $result = [];

        foreach ($primaryArr as $primary) {
            $detail = $this->fetchByPrimaryCache($primary);
            $detail && $result[] = $detail;
        }

        return $result;
    }

    public function fetchAll()
    {
        $result = $this->get($this->getTableName(), $this->paginate, $this->columns);
        $this->resetFilter();
        return empty($result) ? [] : $this->filterShow($result);
    }

    public function fetchAllByCache()
    {
        if (!$this->enableCache)
            return $this->fetchAll();

        $result = [];
        $primaryArr = (array)$this->getValue($this->getTableName(), $this->getPrimaryKey(), $this->paginate);
        $primaryArr && $result = $this->fetchByPrimaryArrCache($primaryArr);

        $this->resetFilter();
        return $result;
    }

    /**
     *[
     *  id=>name,
     *  id=>name
     * ]
     * @return array
     */
    public function fetchPairsByCache()
    {
        $result = [];
        $return = $this->fetchAllByCache();
        if ($return) {
            for ($i = 0; $i < count($return); $i++)
                $result[array_shift($return[$i])] = array_shift($return[$i]);
        }

        return $result;
    }

    public function fetchAssocByCache()
    {
        $result = [];
        $return = $this->fetchAllByCache();
        if ($return) {
            for ($i = 0; $i < count($return); $i++)
                $result[current($return[$i])] = $return[$i];
        }

        return $result;
    }

    public function fetchPairsByPrimaryArrCache($primaryArr)
    {
        $result = [];
        $return = $this->fetchByPrimaryArrCache($primaryArr);
        if ($return) {
            for ($i = 0; $i < count($return); $i++)
                $result[array_shift($return[$i])] = array_shift($return[$i]);
        }

        return $result;
    }

    public function fetchAssocByPrimaryArrCache($primaryArr)
    {
        $result = [];
        $return = $this->fetchByPrimaryArrCache($primaryArr);
        if ($return) {
            for ($i = 0; $i < count($return); $i++)
                $result[current($return[$i])] = $return[$i];
        }

        return $result;
    }

    public function paginateByLimit($page, $perPage)
    {
        $page = (int)$page > 1 ? (int)$page : 1;
        $perPage = (int)$perPage > 0 ? (int)$perPage : 0;

        $this->paginate = [($page - 1) * $perPage, $perPage];
        return $this;
    }

    public function paginateByPrimary($lastPrimary, $perPage, $desc = true)
    {
        $lastPrimary = (int)$lastPrimary;
        if ($lastPrimary > 0) {
            $this->where($this->getPrimaryKey(), $lastPrimary, $desc ? "<" : ">");
            $this->orderBy($this->getPrimaryKey(), $desc ? "DESC" : "ASC");
        }
        $this->paginate = (int)$perPage > 0 ? (int)$perPage : 0;
        return $this;
    }

    public function count()
    {
        return (int)$this->getValue($this->getTableName(), "COUNT(1)");
    }

    public function countDistinct()
    {
        return (int)$this->setQueryOption("DISTINCT")->getValue($this->getTableName(), "COUNT(1)");
    }

    public function countDistinctColumn($column)
    {
        return (int)$this->getValue($this->getTableName(), "COUNT(DISTINCT {$column})");
    }

    public function orderByPrimary($desc = true)
    {
        $this->orderBy($this->getPrimaryKey(), $desc ? "DESC" : "ASC");
        return $this;
    }

    protected function filterInsert($data)
    {
        is_callable($this->filterInsertFunc) && $data = call_user_func_array($this->filterInsertFunc, [$data]);
        return $data;
    }

    public function insertData($insertData)
    {
        return parent::insert($this->getTableName(), $this->filterInsert($insertData));
    }

    public function insertMultiData(array $multiInsertData, array $dataKeys = null)
    {
        return parent::insertMulti($this->getTableName(), $this->filterInsert($multiInsertData), $dataKeys);
    }

    public function replaceData($insertData)
    {
        return parent::replace($this->getTableName(), $this->filterInsert($insertData));
    }

    public function update($tableName, $tableData, $numRows = null)
    {
        return false;
    }

    /**
     * @param $primary
     * @param $tableData
     * @return bool
     */
    public function updateByPrimaryCache($primary, $tableData)
    {
        $this->where($this->getPrimaryKey(), $primary);
        $result = parent::update($this->getTableName(), $this->filterInsert($tableData));
        if ($result && $this->enableCache) {
            if ($this->inTransaction)
                $this->transactionDeleteCachePrimaryArr[] = $primary;
            else
                $this->deleteCache([$primary]);
        }

        return $result;
    }

    public function updateByCache($tableData)
    {
        $affectRows = 0;
        if ($this->enableCache) {
            $primaryArr = (array)$this->getValue($this->getTableName(), $this->getPrimaryKey(), $this->paginate);
            if ($primaryArr) {
                foreach ($primaryArr as $primary) {
                    $ret = $this->updateByPrimaryCache($primary, $tableData);
                    $ret && $affectRows += (int)$this->count;
                }
            }
        } else {
            $result = parent::update($this->getTableName(), $this->filterInsert($tableData), $this->paginate);
            $result && $affectRows = $this->count;
        }

        $this->resetFilter();
        return $affectRows;
    }

    public function increase($primary, $column, $num = 1)
    {
        $inc = $this->inc($num);
        return $this->updateByPrimaryCache($primary, [$column => $inc]);
    }

    public function decrease($primary, $column, $num = 1)
    {
        $dec = $this->dec($num);
        return $this->updateByPrimaryCache($primary, [$column => $dec]);
    }

    public function delete($tableName, $numRows = null)
    {
        return false;
    }

    public function deleteByPrimaryCache($primary)
    {
        $this->where($this->getPrimaryKey(), $primary);
        $result = parent::delete($this->getTableName(), 1);
        if ($result && $this->enableCache) {
            if ($this->inTransaction)
                $this->transactionDeleteCachePrimaryArr[] = $primary;
            else
                $this->deleteCache([$primary]);
        }

        return $result;
    }

    public function deleteByCache()
    {
        $affectRows = 0;

        if ($this->enableCache) {
            $primaryArr = $this->getValue($this->getTableName(), $this->getPrimaryKey(), $this->paginate);
            if ($primaryArr) {
                foreach ($primaryArr as $primary) {
                    $ret = $this->deleteByPrimaryCache($primary);
                    $ret && $affectRows++;
                }
            }
        } else {
            $result = parent::delete($this->getTableName(), $this->paginate);
            $result && $affectRows = is_array($this->paginate) ? $this->paginate[1] : $this->paginate;
        }

        $this->resetFilter();
        return $affectRows;
    }

    public function rawQuery($query, $bindParams = null)
    {
        return false;
    }

    public function startTransaction()
    {
        $this->inTransaction = true;
        $this->transactionDeleteCachePrimaryArr = [];
        parent::startTransaction();
    }

    public function rollback()
    {
        $result = parent::rollback();
        $this->inTransaction = false;
        return $result;
    }

    public function commit()
    {
        $result = parent::commit();
        $this->enableCache && $this->deleteCache($this->transactionDeleteCachePrimaryArr);
        $this->inTransaction = false;
        return $result;
    }
}