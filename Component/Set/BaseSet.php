<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;

use Celltrak\RedisBundle\Component\Client\CelltrakRedis;
use Celltrak\RedisBundle\Component\Multi\Pipeline;

/**
 * Base Set class.
 */
abstract class BaseSet
    implements \Countable, \Iterator, \JsonSerializable
{

    /**
     * @var CelltrakRedis
     * Redis client.
     */
    protected $redis;


    /**
     * @var array
     * Object identifiers contained in Set.
     */
    protected $objectIds;


    /**
     * @param CelltrakRedis $redis
     */
    public function __construct(CelltrakRedis $redis)
    {
        $this->redis = $redis;
        $this->iteratorPosition = 0;
    }

    /**
     * Returns the number of objects in the Set.
     * @return integer
     */
    public abstract function count();

    /**
     * Returns the object identifiers in the Set.
     * @return array
     */
    public abstract function getObjectIds();

    /**
     * Indicates whether object exists in the Set.
     *
     * @param mixed $objectId
     * @return boolean
     */
    public abstract function hasObject($objectId);

    /**
     * Initializes this Set for calculation as part of a UNION or INTERSECTION.
     *
     * @param Pipeline $pipeline
     * @return string   Returns Redis key assigned to the Set.
     */
    public abstract function initializeForCalculation(Pipeline $pipeline);

    /**
     * Creates an INTERSECTION with another Set.
     *
     * @param BaseSet $otherSet
     * @return IntersectionSet
     */
    public function intersect(BaseSet $otherSet)
    {
        $intersection = new IntersectionSet($this->redis);
        $intersection->addSet($this, $otherSet);
        return $intersection;
    }

    /**
     * Creates a UNION with another Set.
     *
     * @param BaseSet $otherSet
     * @return UnionSet
     */
    public function union(BaseSet $otherSet)
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this, $otherSet);
        return $union;
    }

    /**
     * Returns $redis.
     *
     * @return CelltrakRedis
     */
    public function getRedis()
    {
        return $this->redis;
    }

    /**
     * Indicates whether this Set and another share the same Redis client.
     *
     * @param BaseSet $otherSet
     * @return boolean
     */
    public function sharesRedisWithSet(BaseSet $otherSet)
    {
        return $this->redis === $otherSet->getRedis();
    }

    /**
     * {@inheritDoc}
     */
    public function current()
    {
        return $this->objectIds[$this->iteratorPosition];
    }

    /**
     * {@inheritDoc}
     */
    public function key()
    {
        return $this->iteratorPosition;
    }

    /**
     * {@inheritDoc}
     */
    public function next()
    {
        ++$this->iteratorPosition;
    }

    /**
     * {@inheritDoc}
     */
    public function rewind()
    {
        $this->loadObjectIdsOnce();
        $this->iteratorPosition = 0;
    }

    /**
     * {@inheritDoc}
     */
    public function valid()
    {
        return isset($this->objectIds[$this->iteratorPosition]);
    }

    /**
     * {@inheritDoc}
     */
    public function jsonSerialize()
    {
        $this->loadObjectIdsOnce();
        return $this->objectIds;
    }

    /**
     * Loads Set's object identifiers into memory.
     * @return void
     */
    protected abstract function loadObjectIds();

    /**
     * Indicates whether this Set's object identifiers have been loaded into
     * memory.
     * @return boolean
     */
    protected function hasLoadedObjectIds()
    {
        return isset($this->objectIds);
    }

    /**
     * Loads Set's object identifiers into memory unless they've already been
     * loaded.
     * @return boolean Indicates whether object ids were loaded with this call.
     */
    protected function loadObjectIdsOnce()
    {
        if ($this->hasLoadedObjectIds() == false) {
            $this->loadObjectIds();
            return true;
        } else {
            return false;
        }
    }


}
