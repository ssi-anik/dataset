<?php

use Carbon\Carbon;
use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Schema\Blueprint;

class DatabaseStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();
    }

    protected function formatEventName ($name, $type = 'writer') {
        return parent::formatEventName($name, $type);
    }

    protected function rollbackDatabase () {
        $connections = array_keys($this->getDatabaseConnections());

        foreach ( $connections as $connection ) {
            $this->rollbackMigration($connection, 'users');
            $this->rollbackMigration($connection, 'categories');
            $this->rollbackMigration($connection, 'products');
            $this->rollbackMigration($connection, 'orders');
        }
    }

    protected function migrateDatabase () {
        $this->rollbackDatabase();
        $connections = array_keys($this->getDatabaseConnections());

        foreach ( $connections as $connection ) {
            $this->createTableMigration($connection, 'users', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('number');
                $table->string('email');
                $table->timestamps();
            });

            $this->createTableMigration($connection, 'categories', function (Blueprint $table) {
                $table->increments('id');
                $table->string('name');
                $table->string('image_url');
                $table->string('slug');
            });

            $this->createTableMigration($connection, 'products', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('category_id');
                $table->string('name');
                $table->string('image_url');
                $table->smallInteger('price');
            });

            $this->createTableMigration($connection, 'orders', function (Blueprint $table) {
                $table->increments('id');
                $table->smallInteger('user_id');
                $table->string('uuid');
                $table->smallInteger('total_price');
            });
        }
    }

    protected function seedUserTable (array $config = []) {
        $rows = $config['rows'] ?? 30;
        $locale = $config['locale'] ?? 'en_US';
        $connection = $config['connection'] ?? 'default';
        $table = $config['table'] ?? 'users';
        $faker = $this->getFaker($locale);
        $now = Carbon::now();
        $dateVariations = [ 0, 5, 7, 15, 30, 100, 150, 180 ];
        $dates = [];
        foreach ( $dateVariations as $variation ) {
            $dates[] = $now->subDays($variation)->toDateTimeString();
        }

        $data = [];
        foreach ( range(1, $rows) as $i ) {
            $onDate = $dates[rand(0, count($dates)) - 1];
            $data[] = [
                'name'       => $faker->name,
                'email'      => $faker->companyEmail,
                'number'     => $faker->e164PhoneNumber,
                'created_at' => $onDate,
                'updated_at' => $onDate,
            ];
        }

        Manager::connection($connection)->table($table)->insert($data);

        return $data;
    }

    protected function getUserProvider () {
        return (new UserProvider($this->container));
    }

    public function testEventDispatcherIsWorking () {
        $this->addEventListener('test-true', function ($eventName) {
            return $eventName === 'test-true';
        });

        $this->addEventListener('test-count', function (...$payload) {
            return count($payload) === 3;
        });

        $this->assertTrue($this->dispatcher->until('test-true', 'test-true'));
        $this->assertTrue($this->dispatcher->until('test-count', [ 1, true, 'false' ]));
        $this->assertFalse($this->dispatcher->until('test-count'));
        $this->assertTrue(is_null($this->dispatcher->until('no-event-listener')));
    }

    public function testProvidingEventDispatcherFromOutside () {
        $user = $this->getUserProvider();
        $user->setEventDispatcher($this->dispatcher);
        // return false exits program early
        $this->addEventListener($this->formatEventName('starting'), function () {
            return false;
        });

        // call the export method to start execution
        $result = $user->export();

        $this->assertTrue(false === $result);
    }

    public function testCheckIfEventsAreFired () {
        // return false exits program early
        $this->addEventListener($this->formatEventName('starting'), function () {
            return false;
        });
        $received = false;

        // as returned false, it'll shoot exiting
        $this->addEventListener($this->formatEventName('exiting'), function () use (&$received) {
            $received = true;
        });

        // call the export method to start execution
        $result = $this->getUserProvider()->export();

        $this->assertTrue(false === $result);
        $this->assertTrue($received);
    }

    public function testRenamingEventNames () {
        // change the type of the event
        $type = BaseDatabaseStorageProvider::$TYPE = 'writing-type';

        $receivedNewType = false;
        $this->addEventListener($this->formatEventName('starting', $type), function () use (&$receivedNewType) {
            $receivedNewType = true;
        });

        // return false exits program early
        $this->addEventListener($this->formatEventName('preparing_writer', $type), function () {
            return false;
        });

        // call the export method to start execution
        $result = $this->getUserProvider()->export();

        $this->assertTrue(false === $result);
        $this->assertTrue($receivedNewType);
    }

    /*public function testDifferentCsvFilename () {
        $config = [
            'name' => __DIR__ . '/test_company.csv',
        ];

        $this->generateCompaniesData($config);
        BaseCsvStorageProvider::$FILENAME = $config['name'];
        $event = $this->formatEventName('preparing_reader');
        $filenameMatches = false;
        $this->addEventListener($event, function (...$payload) use (&$filenameMatches, $config) {
            if (isset($payload[1]) && $payload[1] == $config['name']) {
                $filenameMatches = true;
            }
        });

        $result = $this->getCompanyProvider()->export();

        $this->assertTrue($filenameMatches);
        $this->assertTrue($result);

        // delete file
        unlink($config['name']);
    }

    public function testDifferentTable () {
        $this->createTableMigration('default', 'company', function (Blueprint $table) {
            $table->increments('id');
            $table->string('name');
            $table->string('image_url');
            $table->string('slug');
        });
        $count = 15;
        $this->generateCompaniesData([ 'lines' => $count ]);
        $table = 'company';
        BaseCsvStorageProvider::$TABLE = $table;
        $result = $this->getCompanyProvider()->export();
        $this->assertTrue($result);

        $this->assertTrue(Manager::table($table)->count() == $count);
        $this->rollbackMigration('default', 'company');
    }

    public function testDifferentDatabaseConnection () {
        $count = 25;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$CONNECTION = 'sqlite';
        $this->getCompanyProvider()->export();

        $this->assertTrue(Manager::connection('sqlite')->table('companies')->count() == $count);
    }

    public function testReadDifferentCsvDelimiter () {
        $count = 10;
        $this->generateCompaniesData([ 'lines' => $count, 'delimiter' => '|' ]);
        BaseCsvStorageProvider::$DELIMITER = '|';
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->export();

        $this->assertTrue(Manager::table('companies')->count() == $count);
    }

    public function testStreamFilters () {
        $count = 10;
        $data = $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$STREAM_FILTERS = [ 'string.toupper' ];
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [ 'slug' => preg_replace('/[^a-z0-9]/i', '-', $record['ADDRESS']) ];
        })->addFilter(function ($record) {
            return [
                'name'      => $record['NAME'],
                'image_url' => $record['IMAGE_URL'],
                'slug'      => $record['NAME'],
            ];
        })->export();

        $this->assertTrue(Manager::table('companies')->find(1)->name == strtoupper($data[1][0]));
    }

    public function testFileOpenMode () {
        $this->generateCompaniesData();
        BaseCsvStorageProvider::$FILE_OPEN_MODE = 'r+';

        $this->assertTrue($this->getCompanyProvider()->export());
    }

    public function testDifferentFileReader () {
        BaseCsvStorageProvider::$HAS_FILE_READER = true;

        $this->assertTrue($this->getCompanyProvider()->addFilter(function ($record) {
            return [
                'name'      => $record['name'],
                'image_url' => $record['image_url'],
                'slug'      => $record['slug'],
            ];
        })->addMutation(function ($record) {
            return [
                'slug' => $record['name'],
            ];
        })->export());
    }

    public function testDoNotSkipEmptyLines () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 5 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->assertFalse($this->getCompanyProvider()->export());
    }

    public function testSkipEmptyLine () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER == 0);
    }

    public function testExceptionIsReceivedByMethod () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER > 0);
    }

    public function testExitOnFailureIfFalse () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 2, 'lines' => 5 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;
        BaseCsvStorageProvider::$EXIT_ON_ERROR = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER == 2);
    }

    public function testExcludeCsvHeaderDealingWithProvidedHeader () {
        $count = 19;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$HEADER_OFFSET = null;
        BaseCsvStorageProvider::$HEADERS = [ 'name', 'address', 'image_url' ];
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->export();

        $this->assertTrue(Manager::table('companies')->count() == $count + 1);
    }

    public function testExcludeCsvHeaderDealingZeroBasedIndex () {
        $count = 10;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$HEADER_OFFSET = null;
        $this->getCompanyProvider()->addFilter(function ($record) {
            return [
                'name'      => $record[0],
                'slug'      => $record[0],
                'image_url' => $record[2],
            ];
        })->addMutation(function ($record) {
            return [];
        })->export();

        $this->assertTrue(Manager::table('companies')->count() == $count + 1);
    }

    public function testCustomHeader () {
        $this->generateCompaniesData();
        BaseCsvStorageProvider::$HEADERS = [ 'NAME', 'ADDRESS', 'IMAGE_URL' ];
        $result = $this->getCompanyProvider()->addFilter(function ($record) {
            return [
                'name'      => $record['NAME'],
                'slug'      => $record['slug'],
                'image_url' => $record['IMAGE_URL'],
            ];
        })->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->export();

        $this->assertTrue($result);
    }

    public function testLimitingCsvRows () {
        $this->generateCompaniesData([ 'lines' => 20 ]);
        BaseCsvStorageProvider::$LIMIT = 3;
        $processedBatch = 0;
        $this->addEventListener('dataset.reader.iteration.batch', function () use (&$processedBatch) {
            ++$processedBatch;
        });
        $this->getCompanyProvider()->export();
        $this->assertTrue($processedBatch == 7);
    }

    public function testDoNotUseDatabaseTransaction () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 10, 'lines' => 15 ]);

        BaseCsvStorageProvider::$SKIP_EMPTY = false;
        BaseCsvStorageProvider::$USE_TRANSACTION = false;

        $result = $this->getCompanyProvider()->export();
        $this->assertFalse($result);
        $this->assertTrue(10 == Manager::table('companies')->count());
    }

    public function testDatabaseTransactionNoInsertionOnFailure () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 7, 'lines' => 10 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->export();
        $this->assertTrue(0 == Manager::table('companies')->count());
    }

    public function testMultipleTableEntries () {
        $this->generateMembersData([ 'lines' => 20, ]);
        BaseCsvStorageProvider::$ENTRIES = true;
        $this->getMemberProvider()->addEntries(function () {
            return [
                'members' => function (Model $model, array $record) {
                    $model->name = $record['name'];
                    $model->age = $record['age'];
                    $model->created_at = date('Y-m-d H:i:s');
                    $model->updated_at = date('Y-m-d H:i:s');
                    $model->save();

                    return $model;
                },
                'phones'  => function (Model $model, array $record, array $previous) {
                    $member = $previous['members'];

                    $model->member_id = $member->id;
                    $model->number = $record['phone'];
                    $model->save();

                    return $model;
                },
                'emails'  => function (Model $model, array $record, array $previous) {
                    $model->member_id = $previous['members']->id;
                    $model->email = $record['email'];
                    $model->save();

                    return $model;
                },
            ];
        })->export();

        $this->assertTrue(Manager::table('members')->count() == 20);
        $this->assertTrue(Manager::table('phones')->count() == 20);
        $this->assertTrue(Manager::table('emails')->count() == 20);
    }

    public function testMultipleTableEntriesCheckingDuplicateBeforeEntry () {
        $this->generateMembersData([
            'lines'             => 50,
            'duplication'       => true,
            'duplication_count' => 5,
        ]);
        BaseCsvStorageProvider::$ENTRIES = true;

        $this->getMemberProvider()->addEntries(function () {
            return [
                'members' => function (Model $model, array $record) {
                    // checks if any row exists with name,
                    $member = Manager::table('members')->where('name', $record['name'])->first();
                    if ($member) {
                        $model->id = $member->id;
                        $model->exists = true;
                    }

                    $model->name = $record['name'];
                    $model->age = $record['age'];
                    $model->created_at = date('Y-m-d H:i:s');
                    $model->updated_at = date('Y-m-d H:i:s');

                    $model->save();

                    return $model;
                },
                'phones'  => function (Model $model, array $record, array $previous) {
                    $member = $previous['members'];

                    $model->member_id = $member->id;
                    $model->number = $record['phone'];
                    $model->save();

                    return $model;
                },
                'emails'  => function (Model $model, array $record, array $previous) {
                    $model->member_id = $previous['members']->id;
                    $model->email = $record['email'];
                    $model->save();

                    return $model;
                },
            ];
        })->export();

        $this->assertTrue(Manager::table('members')->count() == 50);
        $this->assertTrue(Manager::table('phones')->count() == 55);
        $this->assertTrue(Manager::table('emails')->count() == 55);
    }*/
}