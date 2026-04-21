"""
Backfill Entity Types
One-time script to classify existing signals with entity_type and classification.
Run after deploying the entity taxonomy module and applying the SQL migration.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from entity_taxonomy import classify_entity

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

BATCH_SIZE = 500


def backfill():
    """Classify all existing signals and update entity_type + classification."""
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM signals")).scalar()
        print(f"Total signals to classify: {total}")

        offset = 0
        updated = 0
        type_counts = {}
        class_counts = {}

        while offset < total:
            rows = conn.execute(text("""
                SELECT id, content FROM signals
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """), {"limit": BATCH_SIZE, "offset": offset}).fetchall()

            if not rows:
                break

            for row in rows:
                signal_id = row[0]
                content = row[1] or ""

                info = classify_entity(content)
                entity_type = info["entity_type"]
                classification = info["classification"]

                conn.execute(text("""
                    UPDATE signals
                    SET entity_type = :etype, classification = :cls
                    WHERE id = :sid
                """), {
                    "etype": entity_type,
                    "cls": classification,
                    "sid": signal_id,
                })

                type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
                if classification:
                    class_counts[classification] = class_counts.get(classification, 0) + 1

                updated += 1

            conn.commit()
            offset += BATCH_SIZE
            print(f"  Processed {min(offset, total)}/{total}...")

    print(f"\nBackfill complete: {updated} signals classified\n")
    print("Entity type distribution:")
    for etype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {etype}: {count}")

    print("\nClassification distribution:")
    for cls, count in sorted(class_counts.items(), key=lambda x: -x[1]):
        print(f"  {cls}: {count}")


if __name__ == "__main__":
    backfill()
