#include <chrono>
#include <thread>

#include "epoch.h"

namespace taas
{

    EpochManager* EpochManager::em_ = nullptr;

    EpochManager::EpochManager(int epoch_duration = 1) : epoch_duration_(epoch_duration)
    {
        std::atomic_init(&cur_epoch_, 0);
        deconstructor_invoked_ = false;
    }

    EpochManager::~EpochManager()
    {
        deconstructor_invoked_ = true;
        delete em_;
    }

    EpochManager& EpochManager::GetInstance()
    {
        static EpochManager em;
        return em;
    }
    void EpochManager::Run()
    {
        while (!deconstructor_invoked_)
        {
            // 
        }
    }
    void EpochManager::AddPhysicalEpoch()
    {
        cur_epoch_++;
    }
    uint64 EpochManager::GetPhysicalEpoch() 
    {
        return cur_epoch_.load();
    }

    double EpochManager::GetEpochDuration()
    {
        return epoch_duration_;
    }
} // namespace taas




