package finderbots.recommenders.hadoop;

import com.netflix.astyanax.entitystore.EntityManager;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pat
 * Date: 4/22/13
 * Time: 1:14 PM
 * To change this template use File | Settings | File Templates.
 */

@Entity
public class NamedVectorPersistable {
    @Id
    private String id;
    @Column
    private List<Pair<String, Float>> items;

    private EntityManager em;

    private static Logger LOGGER = Logger.getRootLogger();

    /* must have no constructor or one that takes no params for Astyanax
     * public NamedVectorPersistable( String recommendedFor, List<RecommendedItem> recs ){
     */

    public NamedVectorPersistable init(String id, List<Pair<String, Float>> items, EntityManager em) {
        this.id = id;//key for persist
        this.em = em;//persister for this object
        this.items = items;
        return this;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Pair<String, Float>> getItems() {
        return this.items;
    }

    public void setItems(List<Pair<String, Float>> items) {
        this.items = items;
    }

    public EntityManager getEntityManager() {
        return this.em;
    }

    public void save() throws PersistenceException {

        class PairValueComparator implements Comparator<Pair<String, Float>> {

            @Override
            public int compare(Pair<String, Float> o1, Pair<String, Float> o2) {
                return (o1.getSecond()>o2.getSecond() ? -1 : (o1.equals(o2) ? 0 : 1));
            }
        }

        try {
            Collections.sort(this.getItems(), new PairValueComparator());
            this.em.put(this);//does this overwrite if it exists?
        } catch (PersistenceException p) {
            LOGGER.error("Error in NamedVectorPersistable.save() using persister: " + this.em.toString());
            throw p;
        }
    }
}
