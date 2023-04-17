#ifndef EPOCH_H
#define EPOCH_H

#include <atomic>
#include "types.h"

namespace taas
{
    class EpochManager
    {
    private:
        static EpochManager* em_;

        double epoch_duration_; // ms
        std::atomic<uint64> cur_epoch_;
        bool deconstructor_invoked_;

        EpochManager(int epoch_duration);
        ~EpochManager();
        EpochManager(const EpochManager&) = delete;
        EpochManager& operator=(const EpochManager&);
        void Run();
    public:
        static EpochManager& GetInstance();
        void AddPhysicalEpoch();
        uint64 GetPhysicalEpoch();
        double GetEpochDuration();

    };
} // namespace taas



#endif