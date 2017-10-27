<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Query;

/**
 * Utility for passing collection of grouped filters used to query index objects.
 */
class GroupedFilterQuery implements \Iterator
{
    /**
     * @var array
     * Collection of grouped filters.
     */
    protected $groupedFilters;

    /**
     * @var integer
     * Iterator position.
     */
    protected $iteratorPosition;


    public function __construct()
    {
        $this->groupedFilters = [];
        $this->iteratorPosition = 0;
    }

    /**
     * Adds set of grouped filters.
     *
     * @param mixed ...$filters
     * @return void
     */
    public function addGroupedFilters(...$filters)
    {
        $this->groupedFilters[] = $filters;
    }

    /**
     * Returns collection of grouped filters.
     *
     * @return array
     */
    public function getGroupedFilters()
    {
        return $this->groupedFilters;
    }

    /**
     * Returns number of groups represented in collection.
     *
     * @return integer
     */
    public function getGroupCount()
    {
        return count($this->groupedFilters);
    }

    /**
     * {@inheritDoc}
     */
    public function current()
    {
        return $this->groupedFilters[$this->iteratorPosition];
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
        $this->iteratorPosition = 0;
    }

    /**
     * {@inheritDoc}
     */
    public function valid()
    {
        return isset($this->groupedFilters[$this->iteratorPosition]);
    }


}
