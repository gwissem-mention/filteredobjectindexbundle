<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Component\Index;


use CTLib\Component\FilteredObjectIndex\FilteredObjectIndexGroup;
use CTLib\Component\FilteredObjectIndex\FilteredObjectIndexUnionSet;
use CTLib\Component\FilteredObjectIndex\FilteredObjectIndexIntersectionSet;
use CTLib\Component\FilteredObjectIndex\LockedObjectException;


class FilteredObjectIndexGroupTest extends FilteredObjectIndexSetTestCase
{

    const KEY_OBJECT_SALT_LOCK = 'food:obj:salt:lock';


    protected function setUp()
    {
        parent::setUp();

        $this->group = new FilteredObjectIndexGroup($this->indexManager, 'food');
    }


    public function testAddObjectToIndexWithNoFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe');

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
    }

    public function testAddObjectToIndexWithFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dessert', 'dinner']);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe', 'dessert')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe', 'dinner')->hasObject('salt')
        );
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\LockedObjectException
     */
    public function testAddObjectExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->addObjectToIndex('salt', 'recipe', [], -1);
    }

    public function testAddObjectSuccessfulLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);
        $this->group->addObjectToIndex('salt', 'recipe', [], 3);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe');

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->removeObjectFromIndex('salt', 'recipe'));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromIndexWithFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner', 'dessert']);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->removeObjectFromIndex('salt', 'recipe'));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getPersistedSet('recipe', 'dinner')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getPersistedSet('recipe', 'dessert')->hasObject('salt')
        );
    }

    public function testRemoveObjectNotExistingInIndex()
    {
        $this->assertFalse($this->group->removeObjectFromIndex('salt', 'dinner'));
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\LockedObjectException
     */
    public function testRemoveObjectFromIndexExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->removeObjectFromIndex('salt', 'recipe', -1);
    }

    public function testRemoveObjectFromIndexSuccessfulLockWaitTimeout()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);

        $this->assertTrue(
            $this->group->removeObjectFromIndex('salt', 'recipe', 3)
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromAllIndexes()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->group->addObjectToIndex('salt', 'cart');

        $this->assertEqualArrayValues(
            ['recipe', 'cart'],
            $this->group->removeObjectFromAllIndexes('salt')
        );

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertFalse(
            $this->group->isObjectInIndex('salt', 'recipe')
        );
        $this->assertFalse(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->isObjectInIndex('salt', 'cart')
        );
        $this->assertFalse(
            $this->group->getPersistedSet('cart')->hasObject('salt')
        );
    }

    public function testRemoveObjectNotInAnyIndex()
    {
        $this->assertEquals([], $this->group->removeObjectFromAllIndexes('salt'));
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
    }

    public function testMoveObjectToIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->assertEquals(
            ['recipe'],
            $this->group->moveObjectToIndex('salt', 'cart')
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'cart'));
        $this->assertTrue(
            $this->group->getPersistedSet('cart')->hasObject('salt')
        );
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
    }

    public function testMoveObjectToSameIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dessert']);
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe', ['dinner'])
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertTrue(
            $this->group->getPersistedSet('recipe')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getPersistedSet('recipe', 'dinner')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getPersistedSet('recipe', 'dessert')->hasObject('salt')
        );
    }

    public function testMoveObjectNotInExistingIndex()
    {
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe')
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\LockedObjectException
     */
    public function testMoveObjectExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->moveObjectToIndex('salt', 'recipe', [], -1);
    }

    public function testMoveObjectSuccessfulLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe', [], 3)
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
    }

    public function testCreateUnion()
    {
        $union = $this->group->createUnion();

        $this->assertInstanceOf(FilteredObjectIndexUnionSet::class, $union);
    }

    public function testCreateIntersection()
    {
        $inter = $this->group->createIntersection();

        $this->assertInstanceOf(FilteredObjectIndexIntersectionSet::class, $inter);
    }


}
