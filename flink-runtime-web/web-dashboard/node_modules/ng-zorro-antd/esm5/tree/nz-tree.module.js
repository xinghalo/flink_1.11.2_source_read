/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzTreeNodeComponent } from './nz-tree-node.component';
import { NzTreeComponent } from './nz-tree.component';
var NzTreeModule = /** @class */ (function () {
    function NzTreeModule() {
    }
    NzTreeModule.decorators = [
        { type: NgModule, args: [{
                    imports: [
                        CommonModule,
                        NzIconModule,
                        NzNoAnimationModule,
                        NzAddOnModule
                    ],
                    declarations: [
                        NzTreeComponent,
                        NzTreeNodeComponent
                    ],
                    exports: [
                        NzTreeComponent,
                        NzTreeNodeComponent
                    ]
                },] }
    ];
    return NzTreeModule;
}());
export { NzTreeModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidHJlZS9uei10cmVlLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQzNELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDZDQUE2QyxDQUFDO0FBQ2xGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUMvRCxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFFdEQ7SUFBQTtJQWdCMkIsQ0FBQzs7Z0JBaEIzQixRQUFRLFNBQUM7b0JBQ1IsT0FBTyxFQUFPO3dCQUNaLFlBQVk7d0JBQ1osWUFBWTt3QkFDWixtQkFBbUI7d0JBQ25CLGFBQWE7cUJBQ2Q7b0JBQ0QsWUFBWSxFQUFFO3dCQUNaLGVBQWU7d0JBQ2YsbUJBQW1CO3FCQUNwQjtvQkFDRCxPQUFPLEVBQU87d0JBQ1osZUFBZTt3QkFDZixtQkFBbUI7cUJBQ3BCO2lCQUNGOztJQUMwQixtQkFBQztDQUFBLEFBaEI1QixJQWdCNEI7U0FBZixZQUFZIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOekFkZE9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9hZGRvbi9hZGRvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5pbXBvcnQgeyBOelRyZWVOb2RlQ29tcG9uZW50IH0gZnJvbSAnLi9uei10cmVlLW5vZGUuY29tcG9uZW50JztcbmltcG9ydCB7IE56VHJlZUNvbXBvbmVudCB9IGZyb20gJy4vbnotdHJlZS5jb21wb25lbnQnO1xuXG5ATmdNb2R1bGUoe1xuICBpbXBvcnRzICAgICA6IFtcbiAgICBDb21tb25Nb2R1bGUsXG4gICAgTnpJY29uTW9kdWxlLFxuICAgIE56Tm9BbmltYXRpb25Nb2R1bGUsXG4gICAgTnpBZGRPbk1vZHVsZVxuICBdLFxuICBkZWNsYXJhdGlvbnM6IFtcbiAgICBOelRyZWVDb21wb25lbnQsXG4gICAgTnpUcmVlTm9kZUNvbXBvbmVudFxuICBdLFxuICBleHBvcnRzICAgICA6IFtcbiAgICBOelRyZWVDb21wb25lbnQsXG4gICAgTnpUcmVlTm9kZUNvbXBvbmVudFxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56VHJlZU1vZHVsZSB7fVxuIl19