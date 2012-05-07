require 'rubygems'
require 'active_record'
require 'ordered_set'

$:.unshift(File.dirname(__FILE__))
require 'core_ext/active_record/base'

class BatchIds
  DEFAULT_ID_COLUMN  = :id
  DEFAULT_BATCH_SIZE = 1000 unless defined?(DEFAULT_BATCH_SIZE)

  attr_reader :opts, :batch_start_time, :batch_total, :batch_progress

  def self.each_batch(_opts, &block)
    new(_opts).tap do |batch|
      batch.each_batch(&block)
    end
  end

  def self.destroy_tmp_table(_opts)
    opts = parse_opts(_opts)
    opts[:connection].execute( "DROP TABLE IF EXISTS #{opts[:tmp_table_name]} CASCADE" )
  end

  def initialize(_opts)
    @opts = self.class.parse_opts(_opts)
    populate_ids
  end

  def each_batch(&block)
    @batch_progress   = 0
    @batch_total      = count
    @batch_start_time = Time.now

    while(true) do
      batch_ids = next_batch_ids
      break if batch_ids.blank?

      @batch_progress += batch_ids.size
      block.call( batch_ids, self )
    end
  end

  def count(_conditions=nil)
    sql = []
    sql << "SELECT count(*) FROM #{tmp_table_name}"
    sql << "  WHERE #{merge_conditions(_conditions, tmp_table_name)}" unless _conditions.nil?

    result = connection.select_value( sql.join )
    result.to_i unless result.nil?
  end

  def mark_completed(id, result=nil)
    sql = []
    sql << "UPDATE #{tmp_table_name} SET end_time = NOW()"
    sql << ", result = '#{result.to_s.gsub(/'/, "''")}'" unless result.nil?
    sql << " WHERE #{id_column} = #{id}"
    execute( sql.join )
  end

  def destroy_tmp_table
    execute( "DROP TABLE IF EXISTS #{tmp_table_name} CASCADE" )
  end

  [:tmp_table_name, :id_column, :batch_size, :order, :conditions, :connection, :table_name, :reuse_tmp_table].each do |attr|
    define_method(attr) { @opts[ attr ]}
  end

private
  def self.parse_opts(_opts)
    opts = {}

    opts[:table_name]      = _opts[:table_name]
    opts[:table_name]    ||= _opts[:model].table_name if _opts[:model]
    raise(ArgumentError, 'Unknown table name -- must pass :table_name or :model opt') if opts[:table_name].nil? or opts[:table_name].blank?

    opts[:id_column]       = _opts[:id_column]  || DEFAULT_ID_COLUMN
    opts[:batch_size]      = _opts[:batch_size] || DEFAULT_BATCH_SIZE
    opts[:conditions]      = _opts[:conditions]
    opts[:order]           = _opts[:order]      || "#{opts[:id_column]} ASC"
    opts[:order]           = "#{opts[:id_column]} #{opts[:order]}" if ['asc', 'desc'].include?( opts[:order].to_s.downcase )
    opts[:connection]      = _opts[:connection]
    opts[:connection]    ||= _opts[:model].connection if _opts[:model]
    opts[:connection]    ||= ActiveRecord::Base.connection
    #raise(RuntimeError, 'Sorry, only Postgres is supported at this time') if opts[:connection].config[:adapter] != 'postgresql'

    opts[:tmp_table_name]  = ['batch', opts[:table_name], opts[:id_column], _opts[:partition]].compact.join('_')
    opts[:reuse_tmp_table] = _opts[:reuse_tmp_table]

    opts
  end

  def populate_ids
    execute( tmp_table_sql )
  end

  def tmp_table_sql
    sql = []  # join more efficient than string concatenation
    sql << "DROP TABLE IF EXISTS #{tmp_table_name} CASCADE ; " if reuse_tmp_table
    sql << "CREATE TABLE #{tmp_table_name} AS ("
    sql << "  SELECT #{tmp_table_columns_sql} FROM #{sanitize_sql(table_name, table_name)} "
    sql << "  WHERE #{merge_conditions(conditions, table_name)}" unless conditions.nil?
    sql << "  ORDER BY #{order}"
    sql << ")"

    sql.join
  end

  def tmp_table_columns_sql
    [ id_column,
      new_column(:start_time, :timestamp),
      new_column(:end_time,   :timestamp),
      new_column(:result,     'varchar(255)')
    ].join(', ')
  end

  def new_column(col_name, col_type)
    "NULL::#{col_type} AS #{col_name}"
  end

  def next_batch_ids
    sql = []
    sql << "UPDATE #{tmp_table_name} SET start_time = NOW() WHERE #{id_column} IN "
    sql << "(select #{id_column} from #{tmp_table_name} WHERE start_time IS NULL "
    sql << "ORDER BY #{order} LIMIT #{batch_size}) "
    sql << "RETURNING #{id_column}"
    connection.select_values( sql.join )
  end

  def merge_conditions(_conditions, _table_name)
    segments = []

    _conditions.each do |condition|
      unless condition.blank?
        condition[0] = condition[0].to_s if condition.kind_of?(Array)
        sql = sanitize_sql(condition, _table_name)
        segments << sql unless sql.blank?
      end
    end

    "(#{segments.join(') AND (')})" unless segments.empty?
  end

  def sanitize_sql(sql, _table_name)
    ActiveRecord::Base.send(:sanitize_sql, sql, _table_name)
  end

  def execute(sql)
    connection.execute( sql )
  end
end
