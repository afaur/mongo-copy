require 'mongo'

module MongoCollectionCopy
  class Copier

    SPECIFIC_IDS = File.read("#{File.dirname(__FILE__)}/ids.txt}")

    def initialize(source, dest, coll_name)
      @source = Mongo::Connection.from_uri(source)
      @source_db = @source[source.split('/').last]

      #@source_coll = @source_db[coll_name]
      @source_coll_provider        = @source_db['providers']
      @source_coll_provider_assoc  = @source_db['provider_associations']
      @source_coll_provider_award  = @source_db['provider_awards']
      @source_coll_provider_cert   = @source_db['provider_certifications']
      @source_coll_provider_edu    = @source_db['provider_educations']
      @source_coll_provider_exp    = @source_db['provider_expertise']
      @source_coll_provider_lic    = @source_db['provider_licenses']
      @source_coll_provider_mut    = @source_db['provider_mut']
      @source_coll_provider_pos    = @source_db['provider_positions']
      @source_coll_provider_prac   = @source_db['provider_practices']
      @source_coll_provider_pub    = @source_db['provider_publications']
      @source_coll_provider_vid    = @source_db['provider_videos']

      # Always use safe writes on the dest db
      @dest = Mongo::Connection.from_uri(dest + "?safe=true")
      @dest_db = @dest[dest.split('/').last]

      #@dest_coll = @dest_db[coll_name]
      @dest_coll_provider        = @dest_db['providers']
      @dest_coll_provider_assoc  = @dest_db['provider_associations']
      @dest_coll_provider_award  = @dest_db['provider_awards']
      @dest_coll_provider_cert   = @dest_db['provider_certifications']
      @dest_coll_provider_edu    = @dest_db['provider_educations']
      @dest_coll_provider_exp    = @dest_db['provider_expertise']
      @dest_coll_provider_lic    = @dest_db['provider_licenses']
      @dest_coll_provider_mut    = @dest_db['provider_mut']
      @dest_coll_provider_pos    = @dest_db['provider_positions']
      @dest_coll_provider_prac   = @dest_db['provider_practices']
      @dest_coll_provider_pub    = @dest_db['provider_publications']
      @dest_coll_provider_vid    = @dest_db['provider_videos']

      @specific_ids = SPECIFIC_IDS

      puts @specific_ids

      abort("BORK")

      #puts "Source:"
      #puts @source
      #puts @source_db

      #puts "Dest:"
      #puts @dest
      #puts @dest_db

    end

    def run
      while cid = @specific_ids.pop and @specific_ids.size > 0

        # Get the provider doc
        provider_doc   = source_coll_provider.find(       {'_id' => cid}         ).limit(1).first
        # Get the provider assoc doc
        provider_assoc = source_coll_provider_assoc.find( {'provider_id' => cid} ).limit(1).first
        # Get the provider award doc
        provider_award = source_coll_provider_award.find( {'provider_id' => cid} ).limit(1).first
        # Get the provider cert doc
        provider_cert  = source_coll_provider_cert.find(  {'provider_id' => cid} ).limit(1).first
        # Get the provider edu doc
        provider_edu   = source_coll_provider_edu.find(   {'provider_id' => cid} ).limit(1).first
        # Get the provider exp doc
        provider_exp   = source_coll_provider_exp.find(   {'provider_id' => cid} ).limit(1).first
        # Get the provider lic doc
        provider_lic   = source_coll_provider_lic.find(   {'provider_id' => cid} ).limit(1).first
        # Get the provider mut doc
        provider_mut   = source_coll_provider_mut.find(   {'_id' => cid}         ).limit(1).first
        # Get the provider pos doc
        provider_pos   = source_coll_provider_pos.find(   {'provider_id' => cid} ).limit(1).first
        # Get the provider prac doc
        provider_prac  = source_coll_provider_prac.find(  {'provider_id' => cid} ).limit(1).first
        # Get the provider pub doc
        provider_pub   = source_coll_provider_pub.find(   {'provider_id' => cid} ).limit(1).first
        # Get the provider vid doc
        provider_vid   = source_coll_provider_vid.find(   {'provider_id' => cid} ).limit(1).first

        # Write the provider doc
        if provider_doc
          dest_coll_provider.save(       provider_doc )
        end
        # Write the provider assoc doc
        if provider_assoc
          dest_coll_provider_assoc.save( provider_assoc )
        end
        # Write the provider award doc
        if provider_award
          dest_coll_provider_award.save( provider_award )
        end
        # Write the provider cert doc
        if provider_cert
          dest_coll_provider_cert.save(  provider_cert )
        end
        # Write the provider edu doc
        if provider_edu
          dest_coll_provider_edu.save(   provider_edu )
        end
        # Write the provider exp doc
        if provider_exp
          dest_coll_provider_exp.save(   provider_exp )
        end
        # Write the provider lic doc
        if provider_lic
          dest_coll_provider_lic.save(   provider_lic )
        end
        # Write the provider mut doc
        if provider_mut
          dest_coll_provider_mut.save(   provider_mut )
        end
        # Write the provider pos doc
        if provider_pos
          dest_coll_provider_pos.save(   provider_pos )
        end
        # Write the provider prac doc
        if provider_prac
          #provider_prac['ins_plan_ids'] = [-1,-1]
          dest_coll_provider_prac.save(  provider_prac )
        end
        # Write the provider pub doc
        if provider_pub
          dest_coll_provider_pub.save(   provider_pub )
        end
        # Write the provider vid doc
        if provider_vid
          dest_coll_provider_vid.save(   provider_vid )
        end

      end
    end

    def total_dest_documents
      #dest_coll.count
    end

    def total_source_documents
      #source_coll.count
    end

    private

    attr_reader :dest_coll, :source_coll, :dest_coll_provider, :dest_coll_provider_assoc, :dest_coll_provider_award, :dest_coll_provider_cert, :dest_coll_provider_edu, :dest_coll_provider_exp, :dest_coll_provider_lic, :dest_coll_provider_mut, :dest_coll_provider_pos, :dest_coll_provider_prac, :dest_coll_provider_pub, :dest_coll_provider_vid, :source_coll_provider, :source_coll_provider_assoc, :source_coll_provider_award, :source_coll_provider_cert, :source_coll_provider_edu, :source_coll_provider_exp, :source_coll_provider_lic, :source_coll_provider_mut, :source_coll_provider_pos, :source_coll_provider_prac, :source_coll_provider_pub, :source_coll_provider_vid

  end
end
