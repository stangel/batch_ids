class ActiveRecord::Base
  def self.each_batch(opts, &block)
    BatchIds.each_batch( opts.merge(:model => self), &block )
  end
end
