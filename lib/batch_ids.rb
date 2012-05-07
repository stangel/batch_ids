require 'rubygems'
require 'active_record'
require 'ordered_set'

$:.unshift(File.dirname(__FILE__))
require 'core_ext/active_record/base'

class BatchIds
  DEFAULT_ID_COLUMN  = :id
  DEFAULT_BATCH_SIZE = 1000 unless defined?(DEFAULT_BATCH_SIZE)

  attr_reader :opts

  def self.each_batch(opts, &block)
    new(opts).each_batch(&block)
  end

  def initialize(_opts)
    parse_opts(_opts)
    populate_ids
  end

  def each_batch(&block)
    while(true) do
      batch_ids = next_batch_ids
      break if batch_ids.blank?

      block.call( batch_ids )
    end
  end

  def finish(id, result=nil)
    sql = []
    sql << "UPDATE #{opts[:tmp_table_name]} SET end_time = NOW()"
    sql << ", result = '#{result.to_s.gsub(/'/, "''")}'" unless result.nil?
    sql << " WHERE #{opts[:id_column]} = #{id}"
    execute( sql.join )
  end

private
  def parse_opts(_opts)
    @opts = {}

    @opts[:table_name]      = _opts[:table_name]
    @opts[:table_name]    ||= _opts[:model].table_name if _opts[:model]
    raise(ArgumentError, 'Unknown table name -- must pass :table_name or :model opt') if @opts[:table_name].nil? or @opts[:table_name].blank?

    @opts[:id_column]       = _opts[:id_column]  || DEFAULT_ID_COLUMN
    @opts[:batch_size]      = _opts[:batch_size] || DEFAULT_BATCH_SIZE
    @opts[:conditions]      = _opts[:conditions]
    @opts[:order]           = _opts[:order]      || "#{@opts[:id_column]} ASC"
    @opts[:order]           = "#{@opts[:id_column]} #{@opts[:order]}" if ['asc', 'desc'].include?( @opts[:order].to_s.downcase )
    @opts[:connection]      = _opts[:connection]
    @opts[:connection]    ||= _opts[:model].connection if _opts[:model]
    @opts[:connection]    ||= ActiveRecord::Base.connection
    #raise(RuntimeError, 'Sorry, only Postgres is supported at this time') if opts[:connection].config[:adapter] != 'postgresql'

    @opts[:tmp_table_name]  = ['batch', @opts[:table_name], @opts[:id_column], _opts[:partition]].compact.join('_')
    @opts[:reuse_tmp_table] = _opts[:reuse_tmp_table]
  end

  def populate_ids
    opts[:connection].execute( tmp_table_sql )
  end

  def tmp_table_sql
    sql = []  # join more efficient than string concatenation
    sql << "DROP TABLE IF EXISTS #{opts[:tmp_table_name]} CASCADE ; " if opts[:reuse_tmp_table]
    sql << "CREATE TABLE #{opts[:tmp_table_name]} AS ("
    sql << "  SELECT #{tmp_table_columns_sql} FROM #{sanitize_sql(opts[:table_name])} "
    sql << ActiveRecord::Base.merge_conditions(opts[:conditions]) unless opts[:conditions].nil?
    sql << " ORDER BY #{opts[:order]}"
    sql << " )"

    sql.join
  end

  def tmp_table_columns_sql
    [ opts[:id_column],
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
    sql << "UPDATE #{opts[:tmp_table_name]} SET start_time = NOW() WHERE #{opts[:id_column]} IN "
    sql << "(select #{opts[:id_column]} from #{opts[:tmp_table_name]} WHERE start_time IS NULL "
    sql << "ORDER BY #{opts[:order]} LIMIT #{opts[:batch_size]}) "
    sql << "RETURNING #{opts[:id_column]}"
    opts[:connection].select_values( sql.join )
  end

  def sanitize_sql(sql)
    ActiveRecord::Base.send(:sanitize_sql, sql, opts[:table_name])
  end
end
