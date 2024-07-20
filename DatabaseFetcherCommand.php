<?php

namespace App\Console\Commands;

use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\DB;

use function Laravel\Prompts\info;
use function Laravel\Prompts\progress;
use function Laravel\Prompts\spin;

class DatabaseFetcherCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'database:fetch';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Copy all collections from the source MongoDB database to the target MongoDB database, including indexes';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $startTime = microtime(true);

        info('Starting the collection copy process from source to target MongoDB...');

        // Initialize connections
        try {
            $sourceConnection = DB::connection('mongodb_master');
        } catch (Exception $e) {
            Config::set('database.connections.mongodb_master.driver', 'mongodb');
            Config::set('database.connections.mongodb_master.host', '127.0.0.1');
            Config::set('database.connections.mongodb_master.port', '27017');
            Config::set('database.connections.mongodb_master.database', env('DB_DATABASE'));
            Config::set('database.connections.mongodb_master.username', env('DB_USERNAME'));
            Config::set('database.connections.mongodb_master.password', env('DB_PASSWORD'));
            $sourceConnection = DB::connection('mongodb_master');
        }
        try {
            $targetConnection = DB::connection('mongodb');
        } catch (Exception $e) {
            Config::set('database.connections.mongodb.driver', 'mongodb');
            Config::set('database.connections.mongodb.host', '127.0.0.1');
            Config::set('database.connections.mongodb.port', '27017');
            Config::set('database.connections.mongodb.database', env('DB_DATABASE').'_staging');
            Config::set('database.connections.mongodb.username', env('DB_USERNAME'));
            Config::set('database.connections.mongodb.password', env('DB_PASSWORD'));
            $targetConnection = DB::connection('mongodb');
        }

        // Drop all collections in the target MongoDB database
        $this->truncateDatabase($targetConnection);

        // Get the list of collections from the source MongoDB
        $collections = $sourceConnection->getMongoDB()->listCollections();
        $collectionsCount = iterator_count($collections);

        // Reset the cursor after counting
        $collections = $sourceConnection->getMongoDB()->listCollections();

        // Initialize the overall progress bar
        $overallProgressBar = progress(
            label: 'Collections',
            steps: $collectionsCount ? $collectionsCount : 1,
        );
        $overallProgressBar->start();

        foreach ($collections as $collection) {
            $collectionName = $collection->getName();
            info("Processing collection: $collectionName");

            // Fetch total document count for chunking progress bar
            $documentsCount = $sourceConnection->collection($collectionName)->count();
            $chunkProgressBar = progress(
                label: "Chunks for $collectionName",
                steps: $documentsCount ? ceil($documentsCount / 1000) : 1,
            );
            $chunkProgressBar->start();

            // Fetch documents from the source collection in chunks
            $chunkSize = 1000; // Adjust chunk size as needed
            $sourceCursor = $sourceConnection->collection($collectionName)->raw()->find();
            $documents = [];
            foreach ($sourceCursor as $document) {
                $documents[] = (array) $document;
                if (count($documents) >= $chunkSize) {
                    // Insert documents into the target collection
                    $this->insertChunk($targetConnection, $collectionName, $documents);
                    $documents = [];
                    $chunkProgressBar?->advance();
                }
            }
            // Insert remaining documents
            if (! empty($documents)) {
                $this->insertChunk($targetConnection, $collectionName, $documents);
                $chunkProgressBar?->advance();
            }

            // Finish chunk progress bar
            $chunkProgressBar->finish();
            info("Finished processing collection: $collectionName");

            // Replicate indexes
            $this->replicateIndexes($sourceConnection, $targetConnection, $collectionName);

            // Advance overall progress bar
            $overallProgressBar?->advance();
        }

        // Finish overall progress bar
        $overallProgressBar->finish();

        $endTime = microtime(true);
        $duration = $endTime - $startTime;
        $durationFormatted = gmdate('H:i:s', $duration);

        info("Collection copy process completed successfully in {$durationFormatted}.");

        return Command::SUCCESS;
    }

    /**
     * Drop all collections in the target MongoDB database.
     *
     * @param  \Illuminate\Database\Connection  $targetConnection
     * @return void
     */
    protected function truncateDatabase($targetConnection)
    {
        $collections = $targetConnection->getMongoDB()->listCollections();

        foreach ($collections as $collection) {
            $collectionName = $collection->getName();
            $targetConnection->getMongoDB()->dropCollection($collectionName);
            info("Dropped collection: $collectionName");
        }

        info('Target database truncated successfully.');
    }

    /**
     * Insert a chunk of documents into the target collection.
     *
     * @param  \Illuminate\Database\Connection  $targetConnection
     * @param  string  $collectionName
     * @param  array  $documents
     * @return void
     */
    protected function insertChunk($targetConnection, $collectionName, $documents)
    {
        spin(function () use ($targetConnection, $collectionName, $documents) {
            $targetConnection->collection($collectionName)->insert($documents);
        });
    }

    /**
     * Replicate indexes from source to target collection.
     *
     * @param  \Illuminate\Database\Connection  $sourceConnection
     * @param  \Illuminate\Database\Connection  $targetConnection
     * @param  string  $collectionName
     * @return void
     */
    protected function replicateIndexes($sourceConnection, $targetConnection, $collectionName)
    {
        $indexes = $sourceConnection->collection($collectionName)->raw()->listIndexes();

        foreach ($indexes as $index) {
            $key = $index->getKey();
            $options = [];

            foreach ($index as $option => $value) {
                if (! in_array($option, ['ns', 'key'])) {
                    $options[$option] = $value;
                }
            }

            // Remove invalid options for _id index
            if (isset($key['_id'])) {
                unset($options['unique'], $options['sparse']);
            }

            $targetConnection->collection($collectionName)->raw()->createIndex($key, $options);
            info("Created index: {$index->getName()} on collection: $collectionName");
        }
    }
}
