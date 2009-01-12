require 'rubygems'
require "#{File.dirname(__FILE__)}/lib/secretos_lib"
require 'twitter'
require "shorturl"
class Secretos
  include Secretos_lib
  attr_reader :campo,:campo_id
  def initialize
    @twitter = Twitter::Client.new({:login=>CONFIG[:tu_user],:password=> CONFIG[:tu_pass]})
    @categoria_id = categoria_id
    tmp = campos
    @campo = tmp[0]
    @campo_id = tmp[1]
  end
  def secretos_pendientes
    dbh = connect
    row = dbh.select_one "SELECT COUNT(*)COUNT FROM aflus"
    dbh.disconnect
    row['COUNT'].to_i
  end
  def to_
    
  end
  def secretos campo=nil,valor=nil
    dbh = connect
    stm = dbh.execute %{
        SELECT p.aflus_id,pd.post_id, pd.campo_categoria_id campo_id, pd.data
        from
        posts p, aflus a, post_datas pd
        where
        p.categoria_id = #{@categoria_id}
        and
        p.aflus_id = a.id
        and
        pd.post_id = p.id
        #{(campo)? " and p.id in ( select distinct pd.post_id from post_datas pd where pd.campo_categoria_id = #{@campo_id[campo]} and pd.data='#{valor}')":''}
        order by  p.aflus_id,pd.post_id, pd.campo_categoria_id LIMIT 0,#{LIMIT}}
    secrets={}
    stm.each{|row|
      secrets[row['aflus_id']]||={}
      secrets[row['aflus_id']][row['post_id']]||={}
      secrets[row['aflus_id']][row['post_id']][row['campo_id']]=row['data']
    }
    stm.finish
    dbh.disconnect
    secrets
  end
  def categoria_id name='secretos'
    dbh = connect
    row = dbh.select_one "SELECT ID FROM categorias where nombre='#{name}'"
    dbh.disconnect
    row['ID'].to_i
  end
  def campo_id nombre = 'enviado'
    @campo_id[nombre]
  end
  def campos
    campo = {}
    campo_id = {}
    dbh = connect
    stm = dbh.execute "SELECT id,nombre FROM campo_categorias where categoria_id = #{@categoria_id}"
    stm.each{|row| 
      campo_id[row['nombre']]=row['id']
      campo[row['id']]=row['nombre']
    }
    stm.finish
    dbh.disconnect
    [campo,campo_id]
  end
  def to_s
  end
  def actualizar_estado id,estado=ENVIADO
    dbh = connect
    updated = dbh.do "update post_datas set data = '#{estado}' where campo_categoria_id = #{@campo_id['enviado']} and post_id = #{id}" rescue updated=nil
    dbh.commit
    dbh.disconnect
    updated
  end
  
  def postear_chismes chisme
    @twitter.status(:post, chisme)
  end
  def enviar_secreto secreto,para,id
     sent_ok = {}
     para.split(',').each{|mae|
        begin
        mae.strip!
        mae.gsub!('@','')
        url = ShortURL.shorten("#{CONFIG[:ulr_header]}/#{id}")
        sent_ok[mae]=@twitter.message(:post,"#{secreto.strip} (#{url})",mae) if mae && mae!=''
        actualizar_estado id
        postear_chismes "@#{mae} piensan en ti _#{url} #_tu"
        rescue Twitter::RESTError => re
          sent_ok[mae]=false
          actualizar_estado(id,re.code)
        end
     }
     sent_ok
  end
  def enviar_secretos
    secretos('enviado',NO_ENVIADO).each_value{|posts| 
      posts.each{|post_id,campos|
        enviar_secreto campos[@campo_id['secreto']],campos[@campo_id['para']],post_id
      }    
    }
  end
end
secretos = Secretos.new
secretos.enviar_secretos
#result = secretos.enviar_secreto 'secreto auto 1','pabloq,_aflus'
#result.sort.each{|user,status|
#  puts "usuario:#{user}"
#  if status
#    puts "secreto enviado :D (#{status.attributes})"
#  else
#    puts "error, no se envio el secreto..."
#  end
#}
#data =  secretos.secretos('enviado','0')
#data.each{|aflus,posts| 
#  puts "sending to Aflus id #{aflus}"
#  posts.each{|post,campos|
#    puts "post_id=#{post}"
#    campos.each { |a,b|  
#      puts "#{secretos.campo[a]}:#{b}"
#    }
#  }
#}
