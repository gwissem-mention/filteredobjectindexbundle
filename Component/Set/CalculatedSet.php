<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;

use Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException;
use Celltrak\FilteredObjectIndexBundle\Exception\InvalidSetException;
use Celltrak\RedisBundle\Component\Client\CelltrakRedis;
use Celltrak\RedisBundle\Component\Multi\Pipeline;


/**
 * Calculation-based set that uses either UNION or INTERSECTION.
 */
abstract class CalculatedSet extends BaseSet
{
    /**
     * Number of seconds before a temporary calculation set key expires.
     */
    const TEMPORARY_KEY_TTL = 180;

    /**
     * @var array
     * Included children sets used for calculation.
     */
    protected $includedSets;


    /**
     * @param CelltrakRedis $redis
     */
    public function __construct(CelltrakRedis $redis)
    {
        parent::__construct($redis);

        $this->includedSets = [];
    }

    /**
     * Adds Set(s) to include in calculation.
     *
     * @param BaseSet ...$sets
     * @return CalculatedSet Returns $this.
     * @throws InvalidSetException If any of the passed Sets use a different
     *                              Redis client than this Set.
     */
    public function addSet(BaseSet ...$sets)
    {
        foreach ($sets as $set) {
            if ($this->sharesRedisWithSet($set) == false) {
                throw new InvalidSetException("Set doesn't share redis client");
            }

            $this->includedSets[] = $set;
        }

        // Reset objectIds now that set composition has changed.
        unset($this->objectIds);

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function count()
    {
        return count($this->getObjectIds());
    }

    /**
     * {@inheritDoc}
     */
    public function getObjectIds()
    {
        $this->loadObjectIdsOnce();
        return $this->objectIds;
    }

    /**
     * {@inheritDoc}
     */
    public function hasObject($objectId)
    {
        return in_array($objectId, $this->getObjectIds());
    }

    /**
     * {@inheritDoc}
     */
    public function initializeForCalculation(Pipeline $pipeline)
    {
        $destinationSetKey = $this->getTemporarySetKey();
        $this->addCalculateAndStoreCall($pipeline, $destinationSetKey);
        $pipeline->expire($destinationSetKey, self::TEMPORARY_KEY_TTL);

        return $destinationSetKey;
    }

    /**
     * Persists calculation result into new set.
     *
     * @param string $setKey    If not provided, will use GUID-based key.
     *
     * @return PersistedSet
     */
    public function persist($setKey = null)
    {
        $destinationSetKey = $setKey ?: $this->getTemporarySetKey();
        $pipeline = $this->createCalculateAndStorePipeline($destinationSetKey);
        $pipeline->exec();

        return new PersistedSet($this->redis, $destinationSetKey);
    }

    /**
     * Persists calculation result into new temporary set.
     *
     * @param string $setKey    If not provided, will use GUID-based key.
     * @param integer $ttl      Number of seconds before temporary set expires.
     *
     * @return TemporarySet
     */
    public function persistTemporary($setKey = null, $ttl = null)
    {
        $destinationSetKey = $setKey ?: $this->getTemporarySetKey();
        $pipeline = $this->createCalculateAndStorePipeline($destinationSetKey);

        if ($ttl > 0) {
            $pipeline->expire($destinationSetKey, $ttl);
        }

        $pipeline->exec();

        return new TemporarySet($this->redis, $destinationSetKey);
    }

    /**
     * Returns $includedSets.
     * @return array
     */
    public function getIncludedSets()
    {
        return $this->includedSets;
    }

    /**
     * Returns Redis command used to run calculation.
     * @return string
     */
    protected abstract function getCalculateCommand();

    /**
     * Returns Redis command used to run calculation and store result into Redis.
     * @return string
     */
    protected abstract function getCalculateAndStoreCommand();

    /**
     * {@inheritDoc}
     */
    protected function loadObjectIds()
    {
        $pipeline = $this->createPipeline();
        $includedSetKeys = $this->initializeIncludedSetsForCalculation($pipeline);

        $calculateCommand = $this->getCalculateCommand(); // sUnion or sInter
        $pipeline->{$calculateCommand}(...$includedSetKeys)->aliasAs('objectIds');
        $result = $pipeline->exec();
        $this->objectIds = $result->getByAlias('objectIds');
    }

    /**
     * Initializes included children sets required for this set's calculation.
     *
     * @param Pipeline $pipeline
     *
     * @return array
     */
    protected function initializeIncludedSetsForCalculation(Pipeline $pipeline)
    {
        if (empty($this->includedSets)) {
            throw new NoIncludedSetsException();
        }

        $includedSetKeys = [];

        foreach ($this->includedSets as $includedSet) {
            $setKey = $includedSet->initializeForCalculation($pipeline);
            $includedSetKeys[] = $setKey;
        }

        return $includedSetKeys;
    }

    /**
     * Adds Redis call that runs calculation and stores result back into Redis.
     *
     * @param Pipeline $pipeline
     * @param string $destinationSetKey
     *
     * @return void
     */
    protected function addCalculateAndStoreCall(
        Pipeline $pipeline,
        $destinationSetKey
    ) {
        $calculateCommand = $this->getCalculateAndStoreCommand(); // sUnionStore or sInterStore
        $includedSetKeys = $this->initializeIncludedSetsForCalculation($pipeline);

        $pipeline->{$calculateCommand}($destinationSetKey, ...$includedSetKeys);
    }

    /**
     * Creates Redis command pipeline.
     *
     * @return Pipeline
     */
    protected function createPipeline()
    {
        return $this->redis->createPipeline();
    }

    /**
     * Creates Redis command pipeline that runs calculation and stores result
     * back into Redis.
     *
     * @param string $destinationSetKey
     * @return Pipeline
     */
    protected function createCalculateAndStorePipeline($destinationSetKey)
    {
        $pipeline = $this->createPipeline();
        $this->addCalculateAndStoreCall($pipeline, $destinationSetKey);
        return $pipeline;
    }

    /**
     * Returns GUID-based set key.
     *
     * @return string
     */
    protected function getTemporarySetKey()
    {
        return md5(uniqid());
    }

}
