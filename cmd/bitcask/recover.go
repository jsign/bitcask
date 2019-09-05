package main

import (
	"os"

	"github.com/prologic/bitcask"
	"github.com/prologic/bitcask/internal/index"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var recoveryCmd = &cobra.Command{
	Use:     "recover",
	Aliases: []string{"recovery"},
	Short:   "Analyzes and recovers possibly corrupted database and index files",
	Long: `This analyze files to detect different forms of persistence corruption in 
persisted files. It also allows to recover the files to the latest point of integrity.`,
	Args: cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")
		dryRun := viper.GetBool("dry-run")
		os.Exit(recover(path, dryRun))
	},
}

func init() {
	RootCmd.AddCommand(recoveryCmd)
	recoveryCmd.Flags().BoolP("dry-run", "n", false, "Will only check files health without applying recovery if unhealthy")
	viper.BindPFlag("dry-run", recoveryCmd.Flags().Lookup("dry-run"))
}

func recover(path string, dryRun bool) int {
	t, found, err := index.ReadFromFile(path, bitcask.DefaultMaxKeySize, bitcask.DefaultMaxValueSize)
	if err != nil && !index.IsIndexCorruption(err) {
		log.WithError(err).Info("error while opening the index file")
	}
	if !found {
		log.Info("index file doesn't exist, will be recreated on next run.")
		return 0
	}

	if err == nil {
		log.Debug("index file is not corrupted")
		return 0
	}
	log.Debugf("index file is corrupted: %v", err)

	fi, err := os.OpenFile("recovered_index", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.WithError(err).Info("error while creating recovered index file")
		return 1
	}
	defer fi.Close()

	if dryRun {
		log.Debug("dry-run mode, not writing to a file")
		return 0
	}

	err = index.WriteIndex(t, fi)
	if err != nil {
		log.WithError(err).Info("error while writing the recovered index file")
		return 1
	}
	log.Debug("the index was recovered in the recovered_index new file")
	return 0
}
