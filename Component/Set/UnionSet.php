<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;


class UnionSet extends CalculatedSet
{

    /**
     * {@inheritDoc}
     */
    protected function getCalculateCommand()
    {
        return 'sUnion';
    }

    /**
     * {@inheritDoc}
     */
    protected function getCalculateAndStoreCommand()
    {
        return 'sUnionStore';
    }


}
