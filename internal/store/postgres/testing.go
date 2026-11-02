package postgres

import "context"

// ExecForTest runs raw SQL. It exists only so tests in other packages
// can reset the database; application code must use the typed methods.
func (s *Store) ExecForTest(ctx context.Context, sql string, args ...any) error {
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}
