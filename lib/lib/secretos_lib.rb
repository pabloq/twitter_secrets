require "dbi"
require "yaml"
module Secretos_lib
  LIMIT = 1000
  ENVIADO = '1'
  NO_ENVIADO = '0'
  CONFIG = YAML::load_file("#{File.dirname(__FILE__)}/config.yml")
  class Timing
    def initialize
      @t = Time.new
    end
    def elapsed()
      t = Time.new - @t
      @t=nil
      s = (t%60).to_i.to_s
      m = ((t/60) % 60).to_i.to_s
      h = ((t/3600) % 24).to_i.to_s
      out=''
      out = "#{h} hrs" if h and h.to_i>0
      out += " #{m} mins" if m and m.to_i>0
      out += " #{s} secs" if s and s.to_i>0
      out='0 secs' if out==''
      out.strip
    end
  end  
  DBI::DatabaseHandle.class_eval do
    def execute_block(args)
        block = args[:block] || []
        commit = args[:commit] || false
        integrity = args[:integrity] || false
        omit_list = args[:omit_list] || []
        ok = true
        block.each_index{|idx|
          t = Timing.new
          puts "==> #{block[idx][1]}" if block[idx][1]
          begin
            self.execute(block[idx][0])
            self.commit if commit
          rescue DBI::DatabaseError => e
            pretty_query  = ''
            block[idx][0].split("\n").collect{|x|x.strip}.compact.each{|lin| pretty_query+="****#{lin.upcase.strip}"}  if block[idx][0]
            puts "****Error:\n#{pretty_query}"
            puts "****#{e.errstr}"
            ok = false if !omit_list.include?(idx)
          end
          break if (integrity && !ok)
          puts "Time:#{t.elapsed} <=="  if block[idx][1]
        }
        ok
      end   
  end
  Array.class_eval do
    def add_query *value
          value = [value] if !value.instance_of?(Array)
          self.push value
    end
  def to_sql
      query = ''
      self.each{|q|
        q[1].split("\n").collect{|x|x.strip==''?nil:x.strip}.compact.each{|ms|query+="--#{ms.strip}\n"} if q[1]
        q[0].split("\n").collect{|x|x.strip==''?nil:x.strip}.compact.each{|qy|query+="#{qy.upcase.strip}\n"} if q[0]
        query+="/\n" if q[0]
      }
      query
    end
  end
  def connect
    dbh = DBI.connect("DBI:Mysql:#{CONFIG[:db]}:#{CONFIG[:server]}", CONFIG[:user], CONFIG[:pass])
    dbh['AutoCommit'] = false
    dbh
  end
end