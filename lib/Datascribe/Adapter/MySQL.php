<?php 

namespace Datascribe\Adapter;

class MySQL extends \Datascribe\Adapter
{

	protected $_config = array();

	protected $_link;
	
	protected $_result;

	/**
	* Constructor
	*/ 
	public function __construct(array $config)
	{
	
		if (count($config) !== 4)
		{
			throw new InvalidArgumentException('Invalid number of connection parameters.');   
		}

		$this->_config = $config;
	
	}

	/**
	* Conecta ao MySQL.
	*/
	public function connect()
	{

		if ($this->_link === null)
		{

			list($host, $user, $password, $database) = $this->_config;

			if ( ! $this->_link = mysqli_connect($host, $user, $password, $database) )
			{
				throw new \Exception('Error connecting to the server : ' . mysqli_connect_error());
			}
			
			unset($host, $user, $password, $database);

		}
		
		return $this->_link;

	}

	/**
	* Executa uma chamada ao banco.
	*/
	public function query($query)
	{

		if (!is_string($query) || empty($query))
		{
			throw new InvalidArgumentException('The specified query is not valid.');
		}

		$this->connect();

		if ( ! $this->_result = mysqli_query($this->_link, $query) )
		{
			throw new \Exception('Error executing the specified query ' . $query . mysqli_error($this->_link));    
		}

		return $this->_result; 
	
	}

	/**
	* Realiza SELECT.
	*/ 
	public function select($table, $where="", $fields='*', $order='', $limit=null, $offset=null)
	{

		$query = 'SELECT ' . $fields . ' FROM ' . $table
		. (($where) ? ' WHERE ' . $where : '')
		. (($limit) ? ' LIMIT ' . $limit : '')
		. (($offset && $limit) ? ' OFFSET ' . $offset : '')
		. (($order) ? ' ORDER BY '. $order : '') . ';';

		$this->query($query);
	
		return $this->countRows();
	
	}

	/**
	* Perform an INSERT statement
	*/  
	public function insert($table, array $data)
	{
		
		$fields = implode(',', array_keys($data));
		
		$values = implode(',', array_map(array($this, 'quoteValue'), array_values($data)));
		
		$query = 'INSERT INTO ' . $table . ' (' . $fields . ') ' . ' VALUES (' . $values . ')';
		
		$this->query($query);
		
		return $this->getInsertId();

	}

	/**
	* Perform an UPDATE statement
	*/
	public function update($table, array $data, $where = ”)
	{

		$set = array();
		
		foreach ($data as $field => $value)
		{
			$set[] = $field . '=' . $this->quoteValue($value);
		}
		
		$set = implode(',', $set);
		
		$query = 'UPDATE ' . $table . ' SET ' . $set 
		. (($where) ? ' WHERE ' . $where : '');
		
		$this->query($query);

		return $this->getAffectedRows();  

	}

	/**
	* Faz uma chamada para remoção de dados.
	*/
	public function delete($table, $where='')
	{

		$query = 'DELETE FROM ' . $table
		. (($where) ? ' WHERE ' . $where : '');

		$this->query($query);
		
		return $this->getAffectedRows();
	
	}

	/**
	* Escapa um valor especifico.
	*/ 
	public function quoteValue($value)
	{
		
		$this->connect();
		
		if ($value === null)
		{
			$value = 'NULL';
		}
		elseif(!is_numeric($value))
		{
			$value = "’" . mysqli_real_escape_string($this->_link, $value) . "’";
		}
		
		return $value;

	}

	/**
	* Pega uma linha do resultado atual
	* como um array associativo.
	*/
	public function fetch()
	{

		if ($this->_result !== null)
		{
		
			if (($row = mysqli_fetch_array($this->_result, MYSQLI_ASSOC)) === false)
			{
				$this->freeResult();
			}
		
			return $row;
		
		}
		
		return false;

	}

	/**
	* Pega o id do registro inserido.
	*/ 
	public function getInsertId()
	{

		return $this->_link !== null ?
			mysqli_insert_id($this->_link) : null;  
	
	}

	/**
	* Pega o numero de linhas do resultado atual.
	*/  
	public function countRows()
	{

		return $this->_result !== null ?
			mysqli_num_rows($this->_result) : 0;
	
	}

	/**
	* Pega o numero de linhas afetadas.
	*/ 
	public function getAffectedRows()
	{

		return $this->_link !== null ?
			mysqli_affected_rows($this->_link) : 0;
	
	}

	/**
	* Limpa o conjunto de resultados.
	*/ 
	public function freeResult()
	{

		if ($this->_result === null)
		{
			return false;
		}

		mysqli_free_result($this->_result);
		
		return true;

	}

	/**
	* Fecha a conexao com o banco.
	*/ 
	public function disconnect()
	{

		if ($this->_link === null)
		{
			return false;
		}
		
		mysqli_close($this->_link);
		
		$this->_link = null;

		return true;

	}

	/**
	* Close automatically the database connection when the instance of the class is destroyed
	*/ 
	public function __destruct()
	{

		$this->disconnect();

	}

}

?>