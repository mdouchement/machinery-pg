package machinerypg

import "fmt"

func MigrateBroker(url string) error {
	err := GormInit(url)
	if err != nil {
		return fmt.Errorf("MigrateBroker: %s", err.Error())
	}

	db := DB.AutoMigrate(&Task{})
	if db.Error != nil {
		return fmt.Errorf("MigrateBroker: %s", db.Error)
	}

	return nil
}
