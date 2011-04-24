/**
* Copyright: Copyright Alexey Khmara 2010 - 2011
* License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>
* Authors:   Alexey Khmara, alex.khmara@gmail.com
*
* Copyright Alexey Khmara 2010 - 2011. Distributed under Boost License 1.0
* (see accompanying file LICENSE_1_0.txt) or 
* at http://www.boost.org/LICENSE_1_0.txt
*/

module sqlite3;

private import sqlite3_bindings;
private import std.string;
private import std.conv;

private string fromStringz(const char *zString) {
	int i;
	for (i = 0; zString[i]; ++i) {};
	return to!string(zString[0..i]);
}

/**
 * SQLite3 database connection class
 */
class SqliteDatabase
{

	
	/**
	 * Just create object, do not open any databases
	 */
	this(){}
	
	/**
	 * Create object and open (or create) given database file
	 */
	this(string fName) {
		this.open(fName);
	}

	/**
	 * Open or create given database file
	 */
	void open(string fName) {
		int result = sqlite3_open(&(toStringz(fName))[0], &db);
		if(result != SQLITE_OK) {
			throw new Sqlite3Exception("Cannot connect to " ~ fName, result);
		}
	}
	alias open connect;

	/**
	 * Prepare SQL statement, bind arguments if given and run query,
	 * getting first row ready for client.
	 * Client can check isOpen() method to see if there any data in result dataset
	 */
	Sqlite3Statement query(T...)(string sql, T args) {
		sqlite3_stmt *stmt;
		int result = sqlite3_prepare_v2(db, &(toStringz(sql))[0], sql.length, &stmt, null);
		if (result != SQLITE_OK) {
			throw new Sqlite3Exception(sqlite3_errmsg(db), result);
		}
		auto statement = new Sqlite3Statement(this, stmt);
		if (args.length) {
			statement.bind(args);
		}
		statement.step();
		return statement;
	}


	/**
	 * Prepare SQL statement for multiple execution or for paramenters binding.
	 * If args are given, they are bound before return,
	 * so client can immediately call step() to get rows.
	 */
	Sqlite3Statement prepare(T...)(string sql, T args) {
		sqlite3_stmt *stmt;
		int result = sqlite3_prepare_v2(db, &(toStringz(sql))[0], sql.length, &stmt, null);
		if (result != SQLITE_OK) {
			throw new Sqlite3Exception(sqlite3_errmsg(db), result);
		}
		auto statement = new Sqlite3Statement(this, stmt);
		if (args.length) {
			statement.bind(args);
		}
		return statement;
	}

	/**
	 * Execute query without result dataset. Optional arguments can be given to bind
     */
	uint execute(T...)(string sql, T args) {
		sqlite3_stmt *stmt;
		int result = sqlite3_prepare_v2(db, &(toStringz(sql))[0], sql.length, &stmt, null);
		if (result != SQLITE_OK) {
			throw new Sqlite3Exception(sqlite3_errmsg(db), result);
		}
		scope statement = new Sqlite3Statement(this, stmt);
		if (args.length) {
			statement.bind(args);
		}
		statement.step();
		if (sqlite3_column_count(stmt))
			return sqlite3_changes(db);
		else
			return 0;
	}

	alias execute exec;

	/** 
	 * Return last insert id
	 */
	ulong insertId() {
		return sqlite3_last_insert_rowid(db);
	}


	/**
	 * Return the number of rows changed by the last statement
     */
	int changes() {
		return sqlite3_changes(db);
	}

	/**
	 * Close database
	 */
	void close() {
		sqlite3_close(db);
		db = null;
	}



	/**
	 * return original sqlite handle if somethinf exotic needed... Please, try to use 
	 * OO-interface istead/
	 */
	sqlite3* getDb() {
		return this.db;
	}



	/**
	 * Destructor, closes the database if still opened
	 */
	~this() {
		close();
	}

	/**
	 * SQLite3 database handle
	 */
	private sqlite3* db;
}


class Sqlite3Exception : Exception {
public:
	this(string msg, int errCode = 0) {
		super(msg);
		code = errCode;
	}
	this(const (char*) msg, int errCode = 0) {
		super(fromStringz(msg));
		code = errCode;
	}
	int code;
}

/**
 * This class represents SQLite query and allows to use prepared statements and to bind parameters
 */

class Sqlite3Statement {
public:
	enum State { closed = 0, prepared, open};
		
private:
	/**
	 * Constructor that may be called only from this module, specifically - 
	 * from SqliteDatabase.prepare
	 */
	this(SqliteDatabase newParent, sqlite3_stmt *newStmt) {
		stmt = newStmt;
		parent = newParent;
		state = State.prepared;
		colCount = sqlite3_column_count(stmt);
	}
public:
	/*
	 * Bind int value to parameter with index pos
	 */
	void bindOne(uint pos, int value) {
		throwOnError(sqlite3_bind_int(stmt, pos, value));
	}

	/*
	 * Bind long (int64) value to parameter with index pos
	 */
	void bindOne(uint pos, long value) {
		throwOnError(sqlite3_bind_int64(stmt, pos, value));
	}

	/*
	 * Bind double value to parameter with index pos
	 */
	void bindOne(uint pos, double value) {
		throwOnError(sqlite3_bind_double(stmt, pos, value));
	}

	/*
	 * Bind null value to parameter with index pos
	 */
	void bindOne(uint pos, void* value) {
		throwOnError(sqlite3_bind_null(stmt, pos));
	}

	/*
	 * Bind string (text) value to parameter with index pos
	 */
	void bindOne(uint pos, string value) {
		throwOnError(sqlite3_bind_text(stmt, pos, &toStringz(value)[0], value.length, SQLITE_TRANSIENT));
	}

	/*
	 * Bind long (int64) value to parameter by name
	 */
	void bindName(T)(string name, T value) {
		bindOne(arg2pos(arg), value);
	}

	/*
	 * Bind multiple parameters
	 */
	void bind(T...)(T args) {
		foreach(index, arg; args) {
			bindOne(index + 1, arg);
		}
	}
	
	/**
	 * Tries to return next row from query
	 * Return true f row returned, or false if no rows returned
	 * (either if dataset is exhausted or query do not return anything)
	 */

	bool step(T...)(T args) {
		if (args.length)
			bind(args);
		int result = sqlite3_step(stmt);
		if (result ==  SQLITE_DONE) {
			state = State.prepared;
			sqlite3_reset(stmt);
			return false;
		}
		throwOnError(result, SQLITE_ROW);
		state = State.open;
		return true;
	}

	/**
	 * Return true if there is any row ready, false otherwise
	 */
	bool isOpen() {
		return state == State.open;
	}

	/*
	 * Return true if value with given index in curent row is NULL
	 */
	bool isNull(uint col) {
		checkValueRequest(col);
		return (sqlite3_column_type(stmt, col) == SQLITE_NULL);
	}

	/**
	 * Returns value from current row at index col as long (int64).
	 * If type of this value is not int64, conversion occurs.
	 */
	T getValue(T)(uint col)
	if (is(T==long)) {
		checkValueRequest(col);
		return sqlite3_column_int64(stmt, col);
	}

	/**
	 * Returns value from current row at index col as int.
	 * If type of this value is not int64, conversion occurs.
	 */
	T getValue(T)(uint col)
	if (is(T==int)) {
		checkValueRequest(col);
		return sqlite3_column_int(stmt, col);
	}

	/**
	 * Returns value from current row at index col as double.
	 * If type of this value is not int64, conversion occurs.
	 */
	T getValue(T)(uint col)
	if (is(T==double)) {
		checkValueRequest(col);
		return sqlite3_column_double(stmt, col);
	}

	/**
	 * Returns value from current row at index col as string.
	 * If type of this value is not int64, conversion occurs.
	 */
	T getValue(T)(uint col)
	if (is(T==string)) {
		checkValueRequest(col);
		int count = sqlite3_column_bytes(stmt, col);
		return to!string(sqlite3_column_text(stmt, col)[0..count]);
	}
	
	/**
	 * Places current row (or first values from it) into given variables.
	 * If types of variables differ from types of appropriate values in row,
	 * conversion will occur.
	 */
	void getRow(T...)(ref T args) {
		foreach(i, arg; args) {
		args[i] = getValue!(T[i])(i);
		}
	}

	string sql() {
		if (state == State.closed)
			return "";
		else
			return fromStringz(sqlite3_sql(stmt));
	}
	
	/**
	 * Finalizes statement, freing all SQLite structures
	 */
	void close() {
		if (stmt) {
			sqlite3_finalize(stmt);
			stmt = null;
			parent = null;
			colCount = 0;
		}
	}
private:
	/**
	 * Converts name of argumnet to it's index to use in sqlite3_bind_XXX functions
	 */
	int arg2pos(string arg) {
		int pos = sqlite3_bind_parameter_index(stmt, &arg[0]);
		if (!pos)
			throw new Sqlite3Exception("Invalid bind parameter " ~ arg, SQLITE_ERROR);
		return pos;
	}
	/**
	 * Checks, if there is row and if column index is correct, because
	 * else SQLite gives undefined behavior
	 */
	void checkValueRequest(uint col) {
		if (col >= colCount)
			throw new Sqlite3Exception("Invalid column index:" ~to!string(col));
		if (!isOpen)
			throw new Sqlite3Exception("There is no row ready");
	}
	/**
	 * Utility function to simplify SQLite3 function call result checking
	 */
	void throwOnError(int result, int okStatus = SQLITE_OK) {
		if(result != okStatus) {
			throw new Sqlite3Exception(sqlite3_errmsg(parent.getDb()), result);
		}
	}
	/// Underlying SQLite3 statement handle
	sqlite3_stmt *stmt;
	/// Database that created this statement, mainly to use getDb() to get stringified errors
	SqliteDatabase parent;
	/// holds curent statemet state - if it's just prepared, have dataset row ready or closed
	State state;
	/// holds count of rows in the dataset
	int colCount;
}
