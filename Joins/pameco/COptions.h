#ifndef PMC_COPTIONS_H
#define PMC_COPTIONS_H

namespace PMC {
    class COptions
    {
    private:
        enum PMCJoinTechLevel
        {
            PlainVanilla = 0,
            EnhancedSProcessor = 1,
            EnhancedRandSProcessor = 2
        };

    public:

        COptions() : _techLevel(0), _threads(1), _bitRadixLength(24), _maxBitsPerFlipFlopPass(8),
                     _flipFlopCardinality(16000000), _compactHashTableCardinality(16000000),
                     _outputBufferCardinality(1000), _memoryConstraintMB(0), _spinLocks(0), _spinLockDivisor(1024),
                     _relationR(nullptr), _relationS(nullptr), _loadBinaryRelations(false), _doNLJ(false), _bias(0)
        {
        }

//        ~COptions()
//        {
//            if(_relationR != nullptr)
//            {
//                delete _relationR;
//            }
//            if(_relationS != nullptr)
//            {
//                delete _relationS;
//            }
//        }

        double getBias()
        {
            return _bias;
        }

        void setBias(double bias)
        {
            _bias = bias;
        }

        uint32_t getTechLevel()
        {
            return _techLevel;
        }

        void setTechLevel(uint32_t value)
        {
            _techLevel = value;
        }

        bool getLoadBinaryRelations()
        {
            return _loadBinaryRelations;
        }

        void setLoadBinaryRelations(bool value)
        {
            _loadBinaryRelations = value;
        }

        bool getDoNLJ()
        {
            return _doNLJ;
        }

        void setDoNLJ(bool value)
        {
            _doNLJ = value;
        }

        uint32_t getThreads()
        {
            return _threads;
        }

        void setThreads(uint32_t value)
        {
            _threads = value;
        }

        uint32_t getBitRadixLength()
        {
            return _bitRadixLength;
        }

        void setBitRadixLength(uint32_t value)
        {
            _bitRadixLength = value;
        }

        uint32_t getMaxBitsPerFlipFlopPass()
        {
            return _maxBitsPerFlipFlopPass;
        }

        void setMaxBitsPerFlipFlopPass(uint32_t value)
        {
            _maxBitsPerFlipFlopPass = value;
        }

        uint32_t getFlipFlopCardinality()
        {
            return _flipFlopCardinality;
        }

        void setFlipFlopCardinality(uint32_t value)
        {
            _flipFlopCardinality = value;
        }

        uint32_t getCompactHashTableCardinality()
        {
            return _compactHashTableCardinality;
        }

        void setCompactHashTableCardinality(uint32_t value)
        {
            _compactHashTableCardinality = value;
        }

        uint32_t getOutputBufferCardinality()
        {
            return _outputBufferCardinality;
        }

        void setOutputBufferCardinality(uint32_t value)
        {
            _outputBufferCardinality = value;
        }

        relation_t* getRelationR()
        {
            return _relationR;
        }

        void setRelationR(relation_t *value)
        {
            _relationR = value;
        }

        relation_t* getRelationS()
        {
            return _relationS;
        }

        void setRelationS(relation_t *value)
        {
            _relationS = value;
        }


        uint32_t getMemoryConstraintMB()
        {
            return _memoryConstraintMB;
        }

        void setMemoryConstraintMB(uint32_t value)
        {
            _memoryConstraintMB = value;
        }

        uint32_t isTechLevelEnhancedSProcessor()
        {
            return 1;
        }

        uint32_t isTechLevelEnhancedRProcessor()
        {
            return 2;
        }

//	void save(const string &configFile)
//	{
//		ofstream config(configFile);
//
//		config << _techLevel << endl;
//		config << _threads << endl;
//		config << _bitRadixLength << endl;
//		config << _maxBitsPerFlipFlopPass << endl;
//		config << _memoryConstraintMB << endl;
//		config << _flipFlopCardinality << endl;
//		config << _compactHashTableCardinality << endl;
//		config << _outputBufferCardinality << endl;
//		config << boolalpha << _loadBinaryRelations << endl;
//
//		config.close();
//	}
//

        uint32_t getSpinlocks()
        {
            return _spinLocks;
        }

        void setSpinlocks(uint32_t value)
        {
            _spinLocks = value;
        }

        uint32_t getSpinlockDivisor()
        {
            return _spinLockDivisor;
        }

        void setSpinlockDivisor(uint32_t value)
        {
            _spinLockDivisor = value;
        }

    private:
//	bool file_exists(const string &name)
//	{
//		ifstream f(name.c_str());
//
//		if (f.good()) {
//			f.close();
//			return true;
//		} else {
//			f.close();
//			return false;
//		}
//	}

        uint32_t _techLevel;
        uint32_t _threads;
        uint32_t _bitRadixLength;
        uint32_t _maxBitsPerFlipFlopPass;
        uint32_t _flipFlopCardinality;
        uint32_t _compactHashTableCardinality;
        uint32_t _outputBufferCardinality;
        uint32_t _memoryConstraintMB;	// In megabytes
        uint32_t _spinLocks;
        uint32_t _spinLockDivisor;
        relation_t *_relationR;
        relation_t *_relationS;
        bool _loadBinaryRelations;
        bool _doNLJ;
        double _bias;
    };
}

#endif