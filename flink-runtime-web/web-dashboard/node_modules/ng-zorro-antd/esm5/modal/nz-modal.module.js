/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzButtonModule } from '../button/nz-button.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { LoggerModule } from '../core/util/logger/logger.module';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { CssUnitPipe } from './css-unit.pipe';
import { NzModalControlService } from './nz-modal-control.service';
import { NzModalComponent } from './nz-modal.component';
import { NzModalService } from './nz-modal.service';
var NzModalModule = /** @class */ (function () {
    function NzModalModule() {
    }
    NzModalModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, OverlayModule, NzI18nModule, NzButtonModule, LoggerModule, NzIconModule, NzNoAnimationModule],
                    exports: [NzModalComponent],
                    declarations: [NzModalComponent, CssUnitPipe],
                    entryComponents: [NzModalComponent],
                    providers: [NzModalControlService, NzModalService]
                },] }
    ];
    return NzModalModule;
}());
export { NzModalModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbW9kYWwubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1vZGFsL256LW1vZGFsLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXpDLE9BQU8sRUFBRSxjQUFjLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUM1RCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUNsRixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFDakUsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUV0RCxPQUFPLEVBQUUsV0FBVyxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDOUMsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDbkUsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDeEQsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLG9CQUFvQixDQUFDO0FBRXBEO0lBQUE7SUFPNEIsQ0FBQzs7Z0JBUDVCLFFBQVEsU0FBQztvQkFDUixPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxFQUFFLFlBQVksRUFBRSxjQUFjLEVBQUUsWUFBWSxFQUFFLFlBQVksRUFBRSxtQkFBbUIsQ0FBQztvQkFDckgsT0FBTyxFQUFFLENBQUMsZ0JBQWdCLENBQUM7b0JBQzNCLFlBQVksRUFBRSxDQUFDLGdCQUFnQixFQUFFLFdBQVcsQ0FBQztvQkFDN0MsZUFBZSxFQUFFLENBQUMsZ0JBQWdCLENBQUM7b0JBQ25DLFNBQVMsRUFBRSxDQUFDLHFCQUFxQixFQUFFLGNBQWMsQ0FBQztpQkFDbkQ7O0lBQzJCLG9CQUFDO0NBQUEsQUFQN0IsSUFPNkI7U0FBaEIsYUFBYSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpCdXR0b25Nb2R1bGUgfSBmcm9tICcuLi9idXR0b24vbnotYnV0dG9uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLm1vZHVsZSc7XG5pbXBvcnQgeyBMb2dnZXJNb2R1bGUgfSBmcm9tICcuLi9jb3JlL3V0aWwvbG9nZ2VyL2xvZ2dlci5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJMThuTW9kdWxlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcblxuaW1wb3J0IHsgQ3NzVW5pdFBpcGUgfSBmcm9tICcuL2Nzcy11bml0LnBpcGUnO1xuaW1wb3J0IHsgTnpNb2RhbENvbnRyb2xTZXJ2aWNlIH0gZnJvbSAnLi9uei1tb2RhbC1jb250cm9sLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpNb2RhbENvbXBvbmVudCB9IGZyb20gJy4vbnotbW9kYWwuY29tcG9uZW50JztcbmltcG9ydCB7IE56TW9kYWxTZXJ2aWNlIH0gZnJvbSAnLi9uei1tb2RhbC5zZXJ2aWNlJztcblxuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgTnpJMThuTW9kdWxlLCBOekJ1dHRvbk1vZHVsZSwgTG9nZ2VyTW9kdWxlLCBOekljb25Nb2R1bGUsIE56Tm9BbmltYXRpb25Nb2R1bGVdLFxuICBleHBvcnRzOiBbTnpNb2RhbENvbXBvbmVudF0sXG4gIGRlY2xhcmF0aW9uczogW056TW9kYWxDb21wb25lbnQsIENzc1VuaXRQaXBlXSxcbiAgZW50cnlDb21wb25lbnRzOiBbTnpNb2RhbENvbXBvbmVudF0sXG4gIHByb3ZpZGVyczogW056TW9kYWxDb250cm9sU2VydmljZSwgTnpNb2RhbFNlcnZpY2VdXG59KVxuZXhwb3J0IGNsYXNzIE56TW9kYWxNb2R1bGUge31cbiJdfQ==