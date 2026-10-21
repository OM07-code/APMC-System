import { query, closePool } from './pool';
import { generateAuctionId } from '@apmc/shared';
import { logger } from '@apmc/middleware';

// All seed users share the password "password123". The value below is a REAL bcrypt
// hash of that password (cost 10). The previous '$2b$10$testhash' was not a valid
// bcrypt digest, so every seeded login failed bcrypt.compare() at the gateway.
// Regenerate with:  node -e "console.log(require('bcryptjs').hashSync('password123',10))"
const SEED_PASSWORD_HASH = '$2a$10$S.QueCfnaoXfQpP75ODt4OsH3ywf6s5F69Z5VxjDppzbJ8roBwHym';

async function seed(): Promise<void> {
  try {
    // Create a test farmer user. ON CONFLICT updates the hash so re-seeding repairs
    // rows left over from an earlier run that stored the bad placeholder hash.
    const farmerResult = await query(
      `INSERT INTO users (email, password_hash, full_name, role, phone, kyc_status)
       VALUES ($1, $2, $3, 'farmer', $4, 'approved')
       ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash
       RETURNING id`,
      ['farmer@test.com', SEED_PASSWORD_HASH, 'Test Farmer', '+91-9876543210']
    );
    const farmerId = farmerResult.rows[0].id;

    // Create test buyer users
    const buyers = [
      { email: 'buyer1@test.com', name: 'Buyer One', phone: '+91-9876543211' },
      { email: 'buyer2@test.com', name: 'Buyer Two', phone: '+91-9876543212' },
      { email: 'buyer3@test.com', name: 'Buyer Three', phone: '+91-9876543213' },
    ];

    for (const buyer of buyers) {
      await query(
        `INSERT INTO users (email, password_hash, full_name, role, phone, kyc_status)
         VALUES ($1, $2, $3, 'buyer', $4, 'approved')
         ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash`,
        [buyer.email, SEED_PASSWORD_HASH, buyer.name, buyer.phone]
      );
    }

    // Create test auctioneer
    await query(
      `INSERT INTO users (email, password_hash, full_name, role, phone, kyc_status)
       VALUES ($1, $2, $3, 'auctioneer', $4, 'approved')
       ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash`,
      ['auctioneer@test.com', SEED_PASSWORD_HASH, 'Test Auctioneer', '+91-9876543214']
    );

    logger.info('Seed data created successfully');
  } catch (error) {
    logger.error({ err: error }, 'Seed failed');
    throw error;
  } finally {
    await closePool();
  }
}

if (require.main === module) {
  seed()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
}
