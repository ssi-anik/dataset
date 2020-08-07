<?php

use Illuminate\Database\Capsule\Manager;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;

class CsvStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();
        // delete the generated csv files
        $files = [
            __DIR__ . '/Providers/companies.csv',
            __DIR__ . '/Providers/members.csv',
        ];
        foreach ( $files as $file ) {
            if (file_exists($file)) {
                unlink($file);
            }
        }

        // reset states
        BaseCsvStorageProvider::$TYPE = '';
        BaseCsvStorageProvider::$HEADER_OFFSET = 0;
        BaseCsvStorageProvider::$SKIP_EMPTY = true;
        BaseCsvStorageProvider::$EXIT_ON_ERROR = true;
        BaseCsvStorageProvider::$STREAM_FILTERS = [];
        BaseCsvStorageProvider::$LIMIT = 20;
        BaseCsvStorageProvider::$USE_TRANSACTION = true;
        BaseCsvStorageProvider::$ENTRIES = false;
        BaseCsvStorageProvider::$HEADERS = [];
        BaseCsvStorageProvider::$CONNECTION = 'default';
        BaseCsvStorageProvider::$TABLE = '';
        BaseCsvStorageProvider::$FILENAME = '';
        BaseCsvStorageProvider::$DELIMITER = ',';
        BaseCsvStorageProvider::$EXCEPTION_RECEIVED = false;
        BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER = 0;
        BaseCsvStorageProvider::$FILE_OPEN_MODE = 'r';
        BaseCsvStorageProvider::$HAS_FILE_READER = false;
    }

    protected function getCompanyProvider () {
        return (new CompanyProvider($this->container))->addMutation(function ($record) {
            return [ 'slug' => preg_replace('/[^a-z0-9]/i', '-', $record['address']) ];
        })->addFilter(function ($record) {
            unset($record['address']);

            return $record;
        });
    }

    protected function getMemberProvider () {
        return new Member($this->container);
    }

    protected function generateCompaniesData (array $config = []) {
        $filename = $config['name'] ?? __DIR__ . '/Providers/companies.csv';
        $delimiter = $config['delimiter'] ?? ',';
        $emptyLine = $config['empty_line'] ?? false;
        $modulo = $config['modulo'] ?? 8;
        $lines = $config['lines'] ?? 10;
        $skipHeader = $config['skip_header'] ?? false;
        $locale = $config['locale'] ?? 'en_US';
        $faker = $this->getFaker($locale);

        $headers = [ 'name', 'address', 'image_url' ];
        if ($skipHeader) {
            $data = [];
            $rows = [];
        } else {
            $data = [ $headers ];
            $rows = [ implode($delimiter, $headers) ];
        }
        foreach ( range(1, $lines) as $i ) {
            $row = [
                $faker->name,
                $faker->streetAddress,
                $faker->imageUrl(),
            ];

            $data[] = $row;
            $rows[] = implode($delimiter, $row);
            if ($emptyLine && ($i % $modulo === 0)) {
                $data[] = [];
                $rows[] = '';
            }
        }

        file_put_contents($filename, implode(PHP_EOL, $rows));

        return $data;
    }

    protected function generateMembersData (array $config = []) {
        $filename = $config['name'] ?? __DIR__ . '/Providers/members.csv';
        $delimiter = $config['delimiter'] ?? ',';
        $emptyLine = $config['empty_line'] ?? false;
        $modulo = $config['modulo'] ?? 8;
        $lines = $config['lines'] ?? 10;
        $skipHeader = $config['skip_header'] ?? false;
        $locale = $config['locale'] ?? 'en_US';
        $duplication = $config['duplication'] ?? false;
        $duplicationCount = $config['duplication_count'] ?? 1;
        $faker = $this->getFaker($locale);

        $headers = [ 'name', 'age', 'company', 'address', 'phone', 'email', ];
        if ($skipHeader) {
            $data = [];
            $rows = [];
        } else {
            $data = [ $headers ];
            $rows = [ implode($delimiter, $headers) ];
        }
        foreach ( range(1, $lines) as $i ) {
            $row = [
                $faker->name,
                $faker->randomNumber(2),
                $faker->company,
                $faker->streetAddress,
                $faker->e164PhoneNumber,
                $faker->companyEmail,
            ];

            $data[] = $row;
            $rows[] = implode($delimiter, $row);
            if ($emptyLine && ($i % $modulo === 0)) {
                $data[] = [];
                $rows[] = '';
            }
        }

        if ($duplication) {
            // sliced because, if the header is present, that can also be given at random
            $slice = array_slice($data, 1);
            foreach ( (array) array_rand($slice, $duplicationCount) as $index ) {
                // index should always be + 1, because it's sliced from offset 1
                $fromExisting = $data[$index + 1];
                $row = [
                    $fromExisting[0],
                    $faker->randomNumber(2),
                    $faker->company,
                    $faker->streetAddress,
                    $faker->e164PhoneNumber,
                    $faker->companyEmail,
                ];

                $data[] = $row;
                $rows[] = implode($delimiter, $row);
            }
        }

        file_put_contents($filename, implode(PHP_EOL, $rows));

        return $data;
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
        $company = $this->getCompanyProvider();
        $company->setEventDispatcher($this->dispatcher);
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });

        // call the import method to start execution
        $result = $company->import();

        $this->assertTrue(false === $result);
    }

    public function testCheckIfEventsAreFired () {
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });
        $received = false;

        // as returned false, it'll shoot exiting
        $this->addEventListener('dataset.reader.exiting', function () use (&$received) {
            $received = true;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($received);
    }

    public function testRenamingEventNames () {
        // change the type of the event
        BaseCsvStorageProvider::$TYPE = 'reading-type';
        // return false exists program early
        $receivedNewType = false;
        $this->addEventListener('dataset.reading-type.starting', function () use (&$receivedNewType) {
            $receivedNewType = true;
        });

        $this->addEventListener('dataset.reading-type.preparing_reader', function () {
            return false;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($receivedNewType);
    }

    public function testDifferentCsvFilename () {
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

        $result = $this->getCompanyProvider()->import();

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
        $result = $this->getCompanyProvider()->import();
        $this->assertTrue($result);

        $this->assertTrue(Manager::table($table)->count() == $count);
        $this->rollbackMigration('default', 'company');
    }

    public function testDifferentDatabaseConnection () {
        $count = 25;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$CONNECTION = 'sqlite';
        $this->getCompanyProvider()->import();

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
        })->import();

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
        })->import();

        $this->assertTrue(Manager::table('companies')->find(1)->name == strtoupper($data[1][0]));
    }

    public function testFileOpenMode () {
        $this->generateCompaniesData();
        BaseCsvStorageProvider::$FILE_OPEN_MODE = 'r+';

        $this->assertTrue($this->getCompanyProvider()->import());
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
        })->import());
    }

    public function testDoNotSkipEmptyLines () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 5 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->assertFalse($this->getCompanyProvider()->import());
    }

    public function testSkipEmptyLine () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);

        $this->getCompanyProvider()->import();
        $this->assertTrue(BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER == 0);
    }

    public function testExceptionIsReceivedByMethod () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 9 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->import();
        $this->assertTrue(BaseCsvStorageProvider::$HANDLED_EXCEPTION_COUNTER > 0);
    }

    public function testExitOnFailureIfFalse () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 2, 'lines' => 5 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;
        BaseCsvStorageProvider::$EXIT_ON_ERROR = false;

        $this->getCompanyProvider()->import();
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
        })->import();

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
        })->import();

        $this->assertTrue(Manager::table('companies')->count() == $count + 1);
    }

    public function testLimitingCsvRows () {
        $this->generateCompaniesData([ 'lines' => 20 ]);
        BaseCsvStorageProvider::$LIMIT = 3;
        $processedBatch = 0;
        $this->addEventListener('dataset.reader.iteration.batch', function () use (&$processedBatch) {
            ++$processedBatch;
        });
        $this->getCompanyProvider()->import();
        $this->assertTrue($processedBatch == 7);
    }

    public function testDoNotUseDatabaseTransaction () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 10, 'lines' => 15 ]);

        BaseCsvStorageProvider::$SKIP_EMPTY = false;
        BaseCsvStorageProvider::$USE_TRANSACTION = false;

        $result = $this->getCompanyProvider()->import();
        $this->assertFalse($result);
        $this->assertTrue(10 == Manager::table('companies')->count());
    }

    public function testDatabaseTransactionNoInsertionOnFailure () {
        $this->generateCompaniesData([ 'empty_line' => true, 'modulo' => 7, 'lines' => 10 ]);
        BaseCsvStorageProvider::$SKIP_EMPTY = false;

        $this->getCompanyProvider()->import();
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
        })->import();

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
        })->import();

        $this->assertTrue(Manager::table('members')->count() == 50);
        $this->assertTrue(Manager::table('phones')->count() == 55);
        $this->assertTrue(Manager::table('emails')->count() == 55);
    }
}