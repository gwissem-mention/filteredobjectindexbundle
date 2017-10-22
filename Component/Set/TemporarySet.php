<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;


/**
 * Defines Set persisted in Redis for a temporary amount of time.
 */
class TemporarySet extends PersistedSet
{

    /**
     * {@inheritDoc}
     */
    public function __destruct()
    {
        try {
            $this->redis->del($this->persistedSetKey);
        } catch (\Exception $e) { }
    }

}
