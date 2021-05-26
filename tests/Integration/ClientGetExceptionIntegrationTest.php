<?php

/*
 * This file is part of the GraphAware Neo4j Client package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\Tests\Integration;

use GraphAware\Neo4j\Client\ClientBuilder;
use GraphAware\Neo4j\Client\Exception\Neo4jException;
use PHPUnit\Framework\TestCase;

class ClientGetExceptionIntegrationTest extends TestCase
{
    public function testExceptionHandling()
    {
        $boltUrl = 'bolt://neo4j';
        if (isset($_ENV['NEO4J_USER'])) {
            $boltUrl = sprintf(
                'bolt://%s:%s@%s',
                getenv('NEO4J_USER'),
                getenv('NEO4J_PASSWORD'),
                getenv('NEO4J_HOST')
            );
        }

        $client = ClientBuilder::create()
            ->addConnection('default', $boltUrl)
            ->build();

        $this->expectException(Neo4jException::class);
        $result = $client->run('CREATE (n:Cool');
    }
}
